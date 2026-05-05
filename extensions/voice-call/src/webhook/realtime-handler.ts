import { randomUUID } from "node:crypto";
import http from "node:http";
import type { Duplex } from "node:stream";
import { formatErrorMessage } from "openclaw/plugin-sdk/error-runtime";
import {
  buildRealtimeVoiceAgentConsultWorkingResponse,
  createTalkSessionController,
  createRealtimeVoiceBridgeSession,
  REALTIME_VOICE_AGENT_CONSULT_TOOL_NAME,
  type RealtimeVoiceBridgeSession,
  type RealtimeVoiceProviderConfig,
  type RealtimeVoiceProviderPlugin,
  type TalkEvent,
  type TalkEventInput,
  type TalkSessionController,
} from "openclaw/plugin-sdk/realtime-voice";
import WebSocket, { WebSocketServer } from "ws";
import type { VoiceCallRealtimeConfig } from "../config.js";
import type { CallManager } from "../manager.js";
import type { VoiceCallProvider } from "../providers/base.js";
import type { CallRecord, NormalizedEvent } from "../types.js";
import type { WebhookResponsePayload } from "../webhook.types.js";
import {
  RealtimeMulawSpeechStartDetector,
  RealtimeTwilioAudioPacer,
} from "./realtime-audio-pacer.js";

export type ToolHandlerContext = {
  partialUserTranscript?: string;
};
export type ToolHandlerFn = (
  args: unknown,
  callId: string,
  context: ToolHandlerContext,
) => Promise<unknown>;

const STREAM_TOKEN_TTL_MS = 30_000;
const DEFAULT_HOST = "localhost:8443";
const MAX_REALTIME_MESSAGE_BYTES = 256 * 1024;
const MAX_REALTIME_WS_BUFFERED_BYTES = 1024 * 1024;

function normalizePath(pathname: string): string {
  const trimmed = pathname.trim();
  if (!trimmed) {
    return "/";
  }
  const prefixed = trimmed.startsWith("/") ? trimmed : `/${trimmed}`;
  if (prefixed === "/") {
    return prefixed;
  }
  return prefixed.endsWith("/") ? prefixed.slice(0, -1) : prefixed;
}

function buildGreetingInstructions(
  baseInstructions: string | undefined,
  greeting: string | undefined,
): string | undefined {
  const trimmedGreeting = greeting?.trim();
  if (!trimmedGreeting) {
    return undefined;
  }
  const intro =
    "Start the call by greeting the caller naturally. Include this greeting in your first spoken reply:";
  return baseInstructions
    ? `${baseInstructions}\n\n${intro} "${trimmedGreeting}"`
    : `${intro} "${trimmedGreeting}"`;
}

type PendingStreamToken = {
  expiry: number;
  from?: string;
  to?: string;
  direction?: "inbound" | "outbound";
};

type CallRegistration = {
  callId: string;
  initialGreetingInstructions?: string;
};

type ActiveRealtimeVoiceBridge = RealtimeVoiceBridgeSession;

type RealtimeSpeakResult = {
  success: boolean;
  error?: string;
};

function appendRecentTalkEventMetadata(
  call: CallRecord | null | undefined,
  event: TalkEvent,
): void {
  if (!call) {
    return;
  }
  const metadata = call.metadata ?? {};
  const previous = Array.isArray(metadata.recentTalkEvents) ? metadata.recentTalkEvents : [];
  metadata.lastTalkEventAt = event.timestamp;
  metadata.lastTalkEventType = event.type;
  metadata.recentTalkEvents = [
    ...previous,
    {
      id: event.id,
      brain: event.brain,
      mode: event.mode,
      provider: event.provider,
      seq: event.seq,
      sessionId: event.sessionId,
      timestamp: event.timestamp,
      transport: event.transport,
      type: event.type,
      ...(event.turnId ? { turnId: event.turnId } : {}),
      ...(event.final !== undefined ? { final: event.final } : {}),
    },
  ].slice(-12);
  call.metadata = metadata;
}

export class RealtimeCallHandler {
  private readonly toolHandlers = new Map<string, ToolHandlerFn>();
  private readonly pendingStreamTokens = new Map<string, PendingStreamToken>();
  private readonly activeBridgesByCallId = new Map<string, ActiveRealtimeVoiceBridge>();
  private readonly partialUserTranscriptsByCallId = new Map<string, string>();
  private publicOrigin: string | null = null;
  private publicPathPrefix = "";

  constructor(
    private readonly config: VoiceCallRealtimeConfig,
    private readonly manager: CallManager,
    private readonly provider: VoiceCallProvider,
    private readonly realtimeProvider: RealtimeVoiceProviderPlugin,
    private readonly providerConfig: RealtimeVoiceProviderConfig,
    private readonly servePath: string,
  ) {}

  setPublicUrl(url: string): void {
    try {
      const parsed = new URL(url);
      this.publicOrigin = parsed.host;
      const normalizedServePath = normalizePath(this.servePath);
      const normalizedPublicPath = normalizePath(parsed.pathname);
      const idx = normalizedPublicPath.indexOf(normalizedServePath);
      this.publicPathPrefix = idx > 0 ? normalizedPublicPath.slice(0, idx) : "";
    } catch {
      this.publicOrigin = null;
      this.publicPathPrefix = "";
    }
  }

  getStreamPathPattern(): string {
    return `${this.publicPathPrefix}${normalizePath(this.config.streamPath ?? "/voice/stream/realtime")}`;
  }

  buildTwiMLPayload(req: http.IncomingMessage, params?: URLSearchParams): WebhookResponsePayload {
    const host = this.publicOrigin || req.headers.host || DEFAULT_HOST;
    const rawDirection = params?.get("Direction");
    const token = this.issueStreamToken({
      from: params?.get("From") ?? undefined,
      to: params?.get("To") ?? undefined,
      direction: rawDirection?.startsWith("outbound") ? "outbound" : "inbound",
    });
    const wsUrl = `wss://${host}${this.getStreamPathPattern()}/${token}`;
    const twiml = `<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <Connect>
    <Stream url="${wsUrl}" />
  </Connect>
</Response>`;
    return {
      statusCode: 200,
      headers: { "Content-Type": "text/xml" },
      body: twiml,
    };
  }

  handleWebSocketUpgrade(request: http.IncomingMessage, socket: Duplex, head: Buffer): void {
    const url = new URL(request.url ?? "/", "wss://localhost");
    const token = url.pathname.split("/").pop() ?? null;
    const callerMeta = token ? this.consumeStreamToken(token) : null;
    if (!callerMeta) {
      socket.write("HTTP/1.1 401 Unauthorized\r\n\r\n");
      socket.destroy();
      return;
    }

    const wss = new WebSocketServer({
      noServer: true,
      // Reject oversized realtime frames before JSON parsing or bridge setup runs.
      maxPayload: MAX_REALTIME_MESSAGE_BYTES,
    });
    wss.handleUpgrade(request, socket, head, (ws) => {
      let bridge: ActiveRealtimeVoiceBridge | null = null;
      let initialized = false;

      ws.on("message", (data: Buffer) => {
        try {
          const msg = JSON.parse(data.toString()) as Record<string, unknown>;
          if (!initialized && msg.event === "start") {
            initialized = true;
            const startData =
              typeof msg.start === "object" && msg.start !== null
                ? (msg.start as Record<string, unknown>)
                : undefined;
            const streamSid =
              typeof startData?.streamSid === "string" ? startData.streamSid : "unknown";
            const callSid = typeof startData?.callSid === "string" ? startData.callSid : "unknown";
            const nextBridge = this.handleCall(streamSid, callSid, ws, callerMeta);
            if (!nextBridge) {
              return;
            }
            bridge = nextBridge;
            return;
          }
          if (!bridge) {
            return;
          }
          const mediaData =
            typeof msg.media === "object" && msg.media !== null
              ? (msg.media as Record<string, unknown>)
              : undefined;
          if (msg.event === "media" && typeof mediaData?.payload === "string") {
            const audio = Buffer.from(mediaData.payload, "base64");
            bridge.sendAudio(audio);
            if (typeof mediaData.timestamp === "number") {
              bridge.setMediaTimestamp(mediaData.timestamp);
            } else if (typeof mediaData.timestamp === "string") {
              bridge.setMediaTimestamp(Number.parseInt(mediaData.timestamp, 10));
            }
            return;
          }
          if (msg.event === "mark") {
            bridge.acknowledgeMark();
            return;
          }
          if (msg.event === "stop") {
            bridge.close();
          }
        } catch (error) {
          console.error("[voice-call] realtime WS parse failed:", error);
        }
      });

      ws.on("close", () => {
        bridge?.close();
      });

      ws.on("error", (error) => {
        console.error("[voice-call] realtime WS error:", error);
      });
    });
  }

  registerToolHandler(name: string, fn: ToolHandlerFn): void {
    this.toolHandlers.set(name, fn);
  }

  speak(callId: string, instructions: string): RealtimeSpeakResult {
    const bridge = this.activeBridgesByCallId.get(callId);
    if (!bridge) {
      return { success: false, error: "No active realtime bridge for call" };
    }
    try {
      bridge.triggerGreeting(instructions);
      return { success: true };
    } catch (error) {
      return { success: false, error: formatErrorMessage(error) };
    }
  }

  private issueStreamToken(meta: Omit<PendingStreamToken, "expiry"> = {}): string {
    const token = randomUUID();
    this.pendingStreamTokens.set(token, { expiry: Date.now() + STREAM_TOKEN_TTL_MS, ...meta });
    for (const [candidate, entry] of this.pendingStreamTokens) {
      if (Date.now() > entry.expiry) {
        this.pendingStreamTokens.delete(candidate);
      }
    }
    return token;
  }

  private consumeStreamToken(token: string): Omit<PendingStreamToken, "expiry"> | null {
    const entry = this.pendingStreamTokens.get(token);
    if (!entry) {
      return null;
    }
    this.pendingStreamTokens.delete(token);
    if (Date.now() > entry.expiry) {
      return null;
    }
    return {
      from: entry.from,
      to: entry.to,
      direction: entry.direction,
    };
  }

  private handleCall(
    streamSid: string,
    callSid: string,
    ws: WebSocket,
    callerMeta: Omit<PendingStreamToken, "expiry">,
  ): ActiveRealtimeVoiceBridge | null {
    const registration = this.registerCallInManager(callSid, callerMeta);
    if (!registration) {
      ws.close(1008, "Caller rejected by policy");
      return null;
    }

    const { callId, initialGreetingInstructions } = registration;
    const callRecord = this.manager.getCallByProviderCallId(callSid);
    const talk: TalkSessionController = createTalkSessionController({
      sessionId: `voice-call:${callId}:realtime`,
      mode: "realtime",
      transport: "gateway-relay",
      brain: "agent-consult",
      provider: this.realtimeProvider.id,
    });
    const rememberTalkEvent = (event: TalkEvent | undefined): TalkEvent | undefined => {
      if (event) {
        appendRecentTalkEventMetadata(callRecord, event);
      }
      return event;
    };
    const emitTalkEvent = (input: TalkEventInput): TalkEvent => {
      return rememberTalkEvent(talk.emit(input)) as TalkEvent;
    };
    const ensureTalkTurn = (): string => {
      const turn = talk.ensureTurn({
        payload: { callId, providerCallId: callSid },
      });
      rememberTalkEvent(turn.event);
      return turn.turnId;
    };
    const endTalkTurn = (reason = "completed"): void => {
      const ended = talk.endTurn({
        payload: { callId, providerCallId: callSid, reason },
      });
      if (ended.ok) {
        rememberTalkEvent(ended.event);
      }
    };
    const finishOutputAudio = (reason: string): void => {
      rememberTalkEvent(
        talk.finishOutputAudio({
          payload: { callId, providerCallId: callSid, reason },
        }),
      );
    };
    emitTalkEvent({
      type: "session.started",
      payload: { callId, providerCallId: callSid, streamSid },
    });
    console.log(
      `[voice-call] Realtime bridge starting for call ${callId} (providerCallId=${callSid}, initialGreeting=${initialGreetingInstructions ? "queued" : "absent"})`,
    );
    let callEndEmitted = false;
    const emitCallEnd = (reason: "completed" | "error") => {
      if (callEndEmitted) {
        return;
      }
      callEndEmitted = true;
      this.endCallInManager(callSid, callId, reason);
    };

    const sendJson = (message: unknown): boolean => {
      if (ws.readyState !== WebSocket.OPEN) {
        return false;
      }
      if (ws.bufferedAmount > MAX_REALTIME_WS_BUFFERED_BYTES) {
        ws.close(1013, "Backpressure: send buffer exceeded");
        return false;
      }
      ws.send(JSON.stringify(message));
      if (ws.bufferedAmount > MAX_REALTIME_WS_BUFFERED_BYTES) {
        ws.close(1013, "Backpressure: send buffer exceeded");
        return false;
      }
      return true;
    };
    const audioPacer = new RealtimeTwilioAudioPacer({
      streamSid,
      sendJson,
      onBackpressure: () => {
        if (ws.readyState === WebSocket.OPEN) {
          ws.close(1013, "Backpressure: paced audio queue exceeded");
        }
      },
    });
    const speechDetector = new RealtimeMulawSpeechStartDetector();
    const session = createRealtimeVoiceBridgeSession({
      provider: this.realtimeProvider,
      providerConfig: this.providerConfig,
      instructions: this.config.instructions,
      tools: this.config.tools,
      initialGreetingInstructions,
      triggerGreetingOnReady: Boolean(initialGreetingInstructions),
      audioSink: {
        isOpen: () => ws.readyState === WebSocket.OPEN,
        sendAudio: (muLaw) => {
          const turnId = ensureTalkTurn();
          rememberTalkEvent(
            talk.startOutputAudio({
              turnId,
              payload: { callId, providerCallId: callSid },
            }).event,
          );
          emitTalkEvent({
            type: "output.audio.delta",
            turnId,
            payload: { byteLength: muLaw.length },
          });
          audioPacer.sendAudio(muLaw);
        },
        clearAudio: () => {
          audioPacer.clearAudio();
          finishOutputAudio("clear");
        },
        sendMark: (markName) => {
          audioPacer.sendMark(markName);
        },
      },
      onTranscript: (role, text, isFinal) => {
        const turnId = ensureTalkTurn();
        const eventType =
          role === "assistant"
            ? isFinal
              ? "output.text.done"
              : "output.text.delta"
            : isFinal
              ? "transcript.done"
              : "transcript.delta";
        const payload = role === "assistant" ? { text } : { role, text };
        emitTalkEvent({
          type: eventType,
          turnId,
          payload,
          final: isFinal,
        });
        if (role === "user" && isFinal) {
          emitTalkEvent({
            type: "input.audio.committed",
            turnId,
            payload: { callId, providerCallId: callSid },
            final: true,
          });
        }
        if (!isFinal) {
          if (role === "user" && text.trim()) {
            this.partialUserTranscriptsByCallId.set(callId, text);
          }
          return;
        }
        if (role === "user") {
          this.partialUserTranscriptsByCallId.delete(callId);
          const event: NormalizedEvent = {
            id: `realtime-speech-${callSid}-${Date.now()}`,
            type: "call.speech",
            callId,
            providerCallId: callSid,
            timestamp: Date.now(),
            transcript: text,
            isFinal: true,
          };
          this.manager.processEvent(event);
          return;
        }
        this.manager.processEvent({
          id: `realtime-bot-${callSid}-${Date.now()}`,
          type: "call.speaking",
          callId,
          providerCallId: callSid,
          timestamp: Date.now(),
          text,
        });
      },
      onToolCall: (toolEvent, session) => {
        const turnId = ensureTalkTurn();
        emitTalkEvent({
          type: "tool.call",
          turnId,
          itemId: toolEvent.itemId,
          callId: toolEvent.callId,
          payload: { name: toolEvent.name, args: toolEvent.args },
        });
        void this.executeToolCall(
          session,
          callId,
          toolEvent.callId || toolEvent.itemId,
          toolEvent.name,
          toolEvent.args,
          turnId,
          emitTalkEvent,
        );
      },
      onEvent: (event) => {
        if (event.type === "input_audio_buffer.speech_started") {
          ensureTalkTurn();
          return;
        }
        if (event.type === "input_audio_buffer.speech_stopped") {
          const turnId = talk.activeTurnId;
          if (!turnId) {
            return;
          }
          emitTalkEvent({
            type: "input.audio.committed",
            turnId,
            payload: { callId, providerCallId: callSid, source: event.type },
            final: true,
          });
          return;
        }
        if (event.type === "response.done") {
          finishOutputAudio("response.done");
          endTalkTurn("response.done");
          return;
        }
        if (event.type === "error") {
          emitTalkEvent({
            type: "session.error",
            payload: { message: event.detail ?? "Realtime provider error" },
            final: true,
          });
        }
      },
      onReady: () => {
        emitTalkEvent({
          type: "session.ready",
          payload: { callId, providerCallId: callSid },
        });
      },
      onError: (error) => {
        console.error("[voice-call] realtime voice error:", error.message);
        emitTalkEvent({
          type: "session.error",
          payload: { message: error.message },
          final: true,
        });
      },
      onClose: (reason) => {
        this.activeBridgesByCallId.delete(callId);
        this.activeBridgesByCallId.delete(callSid);
        this.partialUserTranscriptsByCallId.delete(callId);
        finishOutputAudio(reason);
        emitTalkEvent({
          type: "session.closed",
          payload: { reason },
          final: true,
        });
        if (reason !== "error") {
          emitCallEnd("completed");
          return;
        }
        emitCallEnd("error");
        if (ws.readyState === WebSocket.OPEN) {
          ws.close(1011, "Bridge disconnected");
        }
        void this.provider
          .hangupCall({ callId, providerCallId: callSid, reason: "error" })
          .catch((error: unknown) => {
            console.warn(
              `[voice-call] Failed to hang up realtime call ${callSid}: ${formatErrorMessage(
                error,
              )}`,
            );
          });
      },
    });
    this.activeBridgesByCallId.set(callId, session);
    this.activeBridgesByCallId.set(callSid, session);
    const sendAudioToSession = session.sendAudio.bind(session);
    session.sendAudio = (audio) => {
      if (speechDetector.accept(audio) && talk.outputAudioActive) {
        const interruptedTurnId = ensureTalkTurn();
        audioPacer.clearAudio();
        finishOutputAudio("barge-in");
        const cancelled = talk.cancelTurn({
          turnId: interruptedTurnId,
          payload: { callId, providerCallId: callSid, reason: "barge-in" },
        });
        if (cancelled.ok) {
          rememberTalkEvent(cancelled.event);
        }
      }
      emitTalkEvent({
        type: "input.audio.delta",
        turnId: ensureTalkTurn(),
        payload: { byteLength: audio.length },
      });
      sendAudioToSession(audio);
    };
    const closeSession = session.close.bind(session);
    session.close = () => {
      this.activeBridgesByCallId.delete(callId);
      this.activeBridgesByCallId.delete(callSid);
      this.partialUserTranscriptsByCallId.delete(callId);
      audioPacer.close();
      closeSession();
    };

    session.connect().catch((error: Error) => {
      console.error("[voice-call] Failed to connect realtime bridge:", error);
      session.close();
      emitCallEnd("error");
      ws.close(1011, "Failed to connect");
    });

    return session;
  }

  private registerCallInManager(
    callSid: string,
    callerMeta: Omit<PendingStreamToken, "expiry"> = {},
  ): CallRegistration | null {
    const timestamp = Date.now();
    const baseFields = {
      providerCallId: callSid,
      timestamp,
      direction: callerMeta.direction ?? "inbound",
      ...(callerMeta.from ? { from: callerMeta.from } : {}),
      ...(callerMeta.to ? { to: callerMeta.to } : {}),
    };

    this.manager.processEvent({
      id: `realtime-initiated-${callSid}`,
      callId: callSid,
      type: "call.initiated",
      ...baseFields,
    });

    const callRecord = this.manager.getCallByProviderCallId(callSid);
    if (!callRecord) {
      return null;
    }

    const initialGreeting = this.extractInitialGreeting(callRecord);
    console.log(
      `[voice-call] Realtime call ${callRecord.callId} initial greeting ${initialGreeting ? "queued" : "absent"}`,
    );
    if (callRecord.metadata) {
      delete callRecord.metadata.initialMessage;
    }

    this.manager.processEvent({
      id: `realtime-answered-${callSid}`,
      callId: callSid,
      type: "call.answered",
      ...baseFields,
    });

    return {
      callId: callRecord.callId,
      initialGreetingInstructions: buildGreetingInstructions(
        this.config.instructions,
        initialGreeting,
      ),
    };
  }

  private extractInitialGreeting(call: CallRecord): string | undefined {
    return typeof call.metadata?.initialMessage === "string"
      ? call.metadata.initialMessage
      : undefined;
  }

  private endCallInManager(callSid: string, callId: string, reason: "completed" | "error"): void {
    this.manager.processEvent({
      id: `realtime-ended-${callSid}-${Date.now()}`,
      type: "call.ended",
      callId,
      providerCallId: callSid,
      timestamp: Date.now(),
      reason,
    });
  }

  private async executeToolCall(
    bridge: ActiveRealtimeVoiceBridge,
    callId: string,
    bridgeCallId: string,
    name: string,
    args: unknown,
    turnId: string,
    emitTalkEvent?: (input: TalkEventInput) => TalkEvent,
  ): Promise<void> {
    const handler = this.toolHandlers.get(name);
    if (
      handler &&
      name === REALTIME_VOICE_AGENT_CONSULT_TOOL_NAME &&
      bridge.bridge.supportsToolResultContinuation &&
      !this.config.fastContext.enabled
    ) {
      emitTalkEvent?.({
        type: "tool.progress",
        turnId,
        callId: bridgeCallId,
        payload: { name, status: "working" },
      });
      bridge.submitToolResult(
        bridgeCallId,
        buildRealtimeVoiceAgentConsultWorkingResponse("caller"),
        { willContinue: true },
      );
    }
    const result = !handler
      ? { error: `Tool "${name}" not available` }
      : await handler(args, callId, {
          partialUserTranscript: this.partialUserTranscriptsByCallId.get(callId),
        }).catch((error: unknown) => ({
          error: formatErrorMessage(error),
        }));
    bridge.submitToolResult(bridgeCallId, result);
    const resultRecord = result as { error?: unknown };
    emitTalkEvent?.({
      type: typeof resultRecord.error === "string" ? "tool.error" : "tool.result",
      turnId,
      callId: bridgeCallId,
      payload: { name, result },
      final: true,
    });
  }
}

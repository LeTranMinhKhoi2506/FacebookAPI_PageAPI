require("dotenv").config();

const express = require("express");
const crypto = require("crypto");
const { Kafka } = require("kafkajs");

const app = express();

const PORT = process.env.PORT || 3001;
const VERIFY_TOKEN = process.env.VERIFY_TOKEN;
const FACEBOOK_APP_SECRET = process.env.FACEBOOK_APP_SECRET;

const KAFKA_BROKERS = (process.env.KAFKA_BROKERS || "localhost:29092")
    .split(",")
    .map((broker) => broker.trim());

const KAFKA_TOPIC = process.env.KAFKA_TOPIC || "raw_events";
const SKIP_SIGNATURE_VERIFY = process.env.SKIP_SIGNATURE_VERIFY === "true";

app.use((req, res, next) => {
    console.log(`[${new Date().toISOString()}] ${req.method} ${req.originalUrl}`);
    next();
});

app.use(
    express.json({
        verify: (req, res, buf) => {
            req.rawBody = buf;
        },
    })
);

const kafka = new Kafka({
    clientId: "webhook-service",
    brokers: KAFKA_BROKERS,
});

const producer = kafka.producer();
let kafkaConnected = false;

function verifyFacebookSignature(req) {
    const signature = req.headers["x-hub-signature-256"];

    if (!signature) {
        console.log("Missing X-Hub-Signature-256");
        return false;
    }

    if (!FACEBOOK_APP_SECRET) {
        console.log("Missing FACEBOOK_APP_SECRET");
        return false;
    }

    if (!signature.startsWith("sha256=")) {
        console.log("Invalid signature format");
        return false;
    }

    const expectedSignature =
        "sha256=" +
        crypto
            .createHmac("sha256", FACEBOOK_APP_SECRET)
            .update(req.rawBody)
            .digest("hex");

    const signatureBuffer = Buffer.from(signature, "utf8");
    const expectedBuffer = Buffer.from(expectedSignature, "utf8");

    if (signatureBuffer.length !== expectedBuffer.length) {
        return false;
    }

    return crypto.timingSafeEqual(signatureBuffer, expectedBuffer);
}

function normalizeFacebookPayload(payload) {
    const events = [];

    const objectName = payload.object || "unknown";

    for (const entry of payload.entry || []) {
        const pageId = entry.id || null;

        const eventTime = entry.time
            ? new Date(entry.time * 1000).toISOString()
            : new Date().toISOString();

        for (const change of entry.changes || []) {
            const value = change.value || {};

            const eventType = value.item || change.field || "unknown";
            const action = value.verb || "unknown";

            const sourceId =
                value.comment_id ||
                value.post_id ||
                value.parent_id ||
                `${Date.now()}-${Math.random().toString(36).substring(2)}`;

            const eventId = `facebook:${eventType}:${action}:${sourceId}`;

            const normalizedEvent = {
                eventId,
                source: "facebook",
                object: objectName,
                page_id: pageId,
                field: change.field || null,

                event_type: eventType,
                action,

                post_id: value.post_id || null,
                comment_id: value.comment_id || null,
                parent_id: value.parent_id || null,
                sender_id: value.sender_id || value.from?.id || null,
                sender_name: value.from?.name || null,
                message: value.message || null,

                event_time: eventTime,
                received_at: new Date().toISOString(),

                raw_payload: payload,
            };

            // Skip events where the Page itself is the sender (e.g. bot replies)
            // This prevents an infinite reply loop:
            //   Page replies → webhook fires → core-service replies again → repeat
            if (
                normalizedEvent.sender_id &&
                normalizedEvent.page_id &&
                normalizedEvent.sender_id === normalizedEvent.page_id
            ) {
                console.log(
                    `[LOOP GUARD] Skipping event ${eventId}: sender_id (${normalizedEvent.sender_id}) matches page_id. This is a bot-generated comment.`
                );
                continue;
            }

            events.push(normalizedEvent);
        }
    }

    return events;
}

app.get("/", (req, res) => {
    res.send("Webhook Service is running");
});

app.get("/health", (req, res) => {
    res.json({
        status: "ok",
        service: "webhook-service",
        kafkaConnected,
        kafkaBrokers: KAFKA_BROKERS,
        kafkaTopic: KAFKA_TOPIC,
    });
});

app.get("/webhook", (req, res) => {
    const mode = req.query["hub.mode"];
    const token = req.query["hub.verify_token"];
    const challenge = req.query["hub.challenge"];

    console.log("Webhook verification request:", {
        mode,
        token,
        challenge,
    });

    if (mode === "subscribe" && token === VERIFY_TOKEN) {
        console.log("Webhook verified successfully");
        return res.status(200).send(challenge);
    }

    console.log("Webhook verification failed");
    return res.sendStatus(403);
});

app.post("/webhook", async (req, res) => {
    try {
        if (!SKIP_SIGNATURE_VERIFY) {
            const isValid = verifyFacebookSignature(req);

            if (!isValid) {
                console.error("Signature validation failed");
                return res.sendStatus(403);
            }
        } else {
            console.log("Signature verification skipped for local testing");
        }

        if (!kafkaConnected) {
            console.error("Kafka producer is not connected");
            return res.sendStatus(503);
        }

        const payload = req.body;

        console.log("Received Facebook payload:");
        console.log(JSON.stringify(payload, null, 2));

        const events = normalizeFacebookPayload(payload);

        if (events.length === 0) {
            console.log("No events found in payload");
            return res.sendStatus(200);
        }

        await producer.send({
            topic: KAFKA_TOPIC,
            messages: events.map((event) => ({
                key: event.eventId,
                value: JSON.stringify(event),
            })),
        });

        console.log(`Published ${events.length} event(s) to Kafka topic ${KAFKA_TOPIC}`);

        return res.sendStatus(200);
    } catch (error) {
        console.error("Webhook error:", error);
        return res.sendStatus(500);
    }
});

async function connectKafka() {
    try {
        await producer.connect();
        kafkaConnected = true;
        console.log("Kafka producer connected");
    } catch (error) {
        kafkaConnected = false;
        console.error("Failed to connect Kafka:", error.message);
    }
}

app.listen(PORT, async () => {
    console.log(`WebhookService running on port ${PORT}`);
    console.log(`Kafka brokers: ${KAFKA_BROKERS.join(", ")}`);
    console.log(`Kafka topic: ${KAFKA_TOPIC}`);

    await connectKafka();
});

process.on("SIGINT", async () => {
    console.log("Shutting down...");
    await producer.disconnect();
    process.exit(0);
});
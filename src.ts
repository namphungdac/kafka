import { Request, Response } from 'express';
import express from "express"
import { KafkaClient, Producer, Consumer } from 'kafka-node';

const app = express();
const port = 3000;

const client = new KafkaClient({ kafkaHost: 'localhost:9092' });
const producer = new Producer(client);
const consumer = new Consumer(
    client,
    [{ topic: 'test', partition: 0 }],
    { autoCommit: false }
);

producer.on('ready', () => {
    console.log('Producer is ready');
});

producer.on('error', (err) => {
    console.error('Producer error:', err);
});

consumer.on('message', (message) => {
    console.log('Received message:', message);
});

consumer.on('error', (err) => {
    console.error('Consumer error:', err);
});

app.use(express.json());

app.post('/send', (req: Request, res: Response) => {
    const { message } = req.body;
    const payloads = [{ topic: 'test', messages: message, partition: 0 }];

    producer.send(payloads, (err, data) => {
        if (err) {
            res.status(500).send(err);
        } else {
            res.send(data);
        }
    });
});

app.listen(port, () => {
    console.log(`Server is running at http://localhost:${port}`);
});

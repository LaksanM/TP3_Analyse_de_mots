import {Kafka, logLevel} from "kafkajs";
import {getLocalBroker} from "../config/config.js";

const isLocalBroker = getLocalBroker()
const redpanda = new Kafka({
    brokers: [
        isLocalBroker ? `${process.env.HOST_IP}:9092` : 'redpanda-0:9092',
        'localhost:19092'],
});
const producer = redpanda.producer();
export const getConnection = async (topic, user, message) => {

    try {
        await producer.connect();
        await producer.send({
            topic: topic,
            messages: [{value: JSON.stringify({message, user})}],
        })
    } catch (error) {
        console.error("Error:", error);
    }

    /*
        await producer.connect();
        await producer.send({
            topic: "chat-room",
            messages: [{ value: JSON.stringify({ message, user }) }],
        })
        return null*/
}
export const disconnect = async () => {
    try {
        await producer.disconnect();
    } catch (error) {
        console.error("Error:", error);
    }
}
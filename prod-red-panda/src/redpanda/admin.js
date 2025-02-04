import {Kafka} from "kafkajs"
import {getLocalBroker} from "../config/config.js";

const isLocalBroker = getLocalBroker()

const redpanda = new Kafka({
    brokers: [
        isLocalBroker ? `${process.env.HOST_IP}:9092` : 'redpanda-0:9092',
        'localhost:19092'],
})

const admin = redpanda.admin()


/***
 *
 * @param topic: string
 * @param partitions: number
 * @param replicas: number
 */
export const createTopic = async (topicName, partitions, replicas) => {
    await admin.connect()
    const existingTopics = await admin.listTopics()

    if (!existingTopics.includes(topicName)) {
        await admin.createTopics({
            topics: [{
                topic: topicName,
                numPartitions: partitions ? partitions : 1,
                replicationFactor: replicas ? replicas : 1,
            }]
        })
    }
    await admin.disconnect()
}
// Import des modules nécessaires
import { Kafka } from 'kafkajs';
import { createClient } from 'redis';
import express from 'express';

// Connexion à Redis
const redisClient = createClient({
    url: 'redis://localhost:6379'
});

redisClient.on('error', (err) => console.error('Erreur de connexion à Redis :', err));
await redisClient.connect();
console.log('Connecté à Redis');

// Connexion à Kafka
const kafka = new Kafka({
    clientId: 'my-consumer',
    brokers: ['localhost:19092'],
});

const consumer = kafka.consumer({ groupId: 'test-group' });

// Fonction de formatage du timestamp
const formatTimestamp = (timestamp) => {
    const date = new Date(Number(timestamp));
    const day = String(date.getDate()).padStart(2, '0');
    const month = String(date.getMonth() + 1).padStart(2, '0');
    const year = date.getFullYear();
    const hours = String(date.getHours()).padStart(2, '0');
    const minutes = String(date.getMinutes()).padStart(2, '0');
    return `${day}/${month}/${year} à ${hours}:${minutes}`;
};

// Fonction principale
const run = async () => {
    await consumer.connect();
    console.log('Connecté à RedPanda');

    await consumer.subscribe({ topic: 'mon-super-topic', fromBeginning: true });
    console.log('Abonné au topic : mon-super-topic');

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            const formattedDate = formatTimestamp(message.timestamp);
            const data = JSON.parse(message.value.toString());

            console.log(`Message reçu à ${formattedDate} :`, data);

            // Découpage du message en mots
            const words = data.message.split(/\s+/);

            // Incrément des occurrences dans Redis
            for (const word of words) {
                const cleanWord = word.toLowerCase().replace(/[^a-zA-Z0-9]/g, '');
                if (cleanWord) {
                    await redisClient.incr(cleanWord);
                    console.log(`Compteur incrémenté pour le mot : "${cleanWord}"`);
                }
            }
        },
    });
};

run().catch(console.error);

// Serveur HTTP pour récupérer les statistiques
const app = express();
const PORT = 3000;

app.get('/stats', async (req, res) => {
    try {
        const keys = await redisClient.keys('*');
        const stats = {};

        for (const key of keys) {
            const count = await redisClient.get(key);
            stats[key] = parseInt(count, 10);
        }

        res.json(stats);
    } catch (error) {
        console.error('Erreur lors de la récupération des stats :', error);
        res.status(500).json({ error: 'Erreur interne du serveur' });
    }
});

app.listen(PORT, () => {
    console.log(`Serveur HTTP démarré sur http://localhost:${PORT}`);
});

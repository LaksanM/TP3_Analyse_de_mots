## Client Redpanda (Producer) sous NodeJS
Le client envoie des données aléatoires numériques ou textuelles en fonction de la configuration enregistrée.
Il est prévu de l'utiliser avec le docker-compose `redpanda-quickstart`. 
L'intérêt de ce stack est d'intégrer directement le `docker network redpanda-network`
nécessaire pour la communication inter-container.
Ce réseau devra être spécifié lors du lancement du conteneur **red-panda-producer**.

### Rôle des définitions du fichier .env
* `HOST_IP` pour définir un broker Redpanda. Par défaut `redpanda-0:9092` et `localhost:19092`
  * Exemple :
      ```.dotenv
    HOST_IP=172.16.0.23
      ```
* `TOPIC` pour définir le topic de communication. Par défaut, `topic-de-test`.
  * Exemple :
      ```.dotenv
    TOPIC="mon-super-topic"
      ```
* `NUMBER_WORD` pour définir le nombre de mots fourni pour une génération en mode texte. Par défaut, `3`
  * Exemple :
      ```.dotenv
    NUMBER_WORD=5
      ```
* `INTEGER_SIZE` pour définir la taille du nombre fourni avant la virgule en mode nombre. Par défaut, `2`
  * Exemple :
      ```.dotenv
    INTEGER_SIZE=3
      ```
* `DECIMAL_VALUE` pour définir le nombre de chiffre après la virugule en mode nombre. Par défaut, `0`
  * Exemple :
      ```.dotenv
    DECIMAL_VALUE=4
      ```
* `SIGNED` pour définir si le nombre fourni est signé en mode nombre (`true` ou `false`). Par défaut, `false`
  * Exemple :
      ```.dotenv
    SIGNED=true
      ```
* `PERIODE_MS` pour définir période d'envoie de message en millisecondes. Par défaut, `5000`
  * Exemple :
      ```.dotenv
    PERIODE_MS=3000
      ```
* `MESSAGE` pour définir le type de message à envoyer entre `nombre` ou `texte`. Par défaut, `texte`
  * Exemple :
      ```.dotenv
    MESSAGE=nombre
      ```
* `DEBUG` pour définir le mode debug qui affiche sur la console le message, le topic et l'utilisateur emetteur. Par défaut, `false`
  * Exemple :
      ```.dotenv
    DEBUG=true
      ```

### Utilisation avec Docker
* Pour construire le conteneur :

```docker
docker build --tag red-panda-producer  . 
```

* Pour démarrer le conteneur :
```docker
docker run -d --name RedPanda-Producer --volume=./.env:/home/node/app/.env --network=redpanda-quickstart_redpanda_network red-panda-producer
```

### Pour un usage sans Docker
* Pour démarrer l'applicatif
```shell
npm start
```

* Pour démarrer l'applicatif en mode developper (node --watch)

En thérorie inutile, mais disponible pour la forme...
```shell
npm run dev
```

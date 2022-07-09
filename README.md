# Spring Kafka Error Handler POCs

# Introdução

Neste repositório temos alguns projetos (módulos) para testar algumas técnicas de tratamento de erros no spring kafka.

## Imagem Docker landoop fast-data-dev

O projeto usa essa imagem que já vem com UI, kafka-connect e schema-registry. Perfeito para testes.

inicie com o comando:

```bash
docker-compose up -d
```

Para abrir a UI do kafka, acesse:

- http://localhost:3030

## Testando

Após iniciar a aplicação, envie as seguintes palavras no kafka:

```bash
echo -e "hello\nkafka\nconsumer\nerror\nhello\nkafka\nconsumer" | kafkacat -P -b localhost:9092 -t words-batch
```

Foi usado kafkacat, mas também poderiamos usar kafka-console-producer entrando no cluster e enviando por lá:

```bash
docker exec -it spring-kafka-error-handler_kafka-cluster_1 bash
echo -e "algumas\nmensagens\nenviadas\nerror\nmais\nmensagens\nenviadas" | kafka-console-producer --bootstrap-server localhost:9092 --topic words-batch
```

## Como funciona

O código está testando o tratamento de erros padrão do spring kafka batch.

O consumer está programado para dar erro quando encontrar a palavra "error"

Primeiro, vai ler um lote de 7 mensagens e processar as 3 primeiras com sucesso.

A quarta mensagem ira falhar, então fara 10 tentativas, sempre buscando um lote de 4 records/mensagens (lembrando que 3 já foram processadas)

Após falhar 10 vezes, vai desistir do record 'problematico' e ira buscar as 3 ultimas mensagens e, dessa vez, processando com sucesso!


O codigo esta baseado nesse tutorial:

- https://www.codewithdilip.com/articles/springboot/batchconsumer_erorhandling

Temos outros exemplos aqui tambem:

- https://github.com/rlanhellas/tdc2021-consumer-kafka-resiliente-retry-simples
- https://github.com/rlanhellas/tdc2021-consumer-kafka-resiliente-deadletter
- https://github.com/rlanhellas/tdc2021-consumer-kafka-resiliente-retry-seletivo

## Consideraçoes

Tratamento de erros no kafka é importante, entao bora estudar esse assunto que nao e todo mundo que manja e/ou usa de forma adequada.


Willams "osdeving" Sousa

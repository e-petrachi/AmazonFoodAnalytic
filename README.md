# AMAZON FOOD ANALYTIC

Si consideri il dataset [Amazon Fine Food Reviews](https://www.kaggle.com/snap/amazon-fine-food-reviews), 
che contiene circa 600.000 recensioni di prodotti gastronomici rilasciati su Amazon 
dal 1999 al 2012. Il dataset è in formato CSV e ogni riga ha i seguenti campi:

* Id,
* ProductId (unique identifier for the product),
* UserId (unique identifier for the user),
* ProfileName,
* HelpfulnessNumerator (number of users who found the review helpful),
* HelpfulnessDenominator (number of users who graded the review),
* Score (rating between 1 and 5),
* Time (timestamp of the review expressed in Unix time),
* Summary (summary of the review),
* Text (text of the review).

## STRUTTURA 

Il progetto in JAVA contiene rispettivamente in MapReduce:

* [1.](/src/main/java/mapreduce/job1/AmazonFoodAnalytic.java) Un job che sia in grado di generare, per ciascun anno, le dieci parole che sono state più usate nelle recensioni (campo summary) in ordine di frequenza, indicando, per ogni parola, la sua frequenza, ovvero il numero di occorrenze della parola nelle recensioni di quell’anno.

* [2.](/src/main/java/job2/AmazonFoodAnalytic.java) Un job che sia in grado di generare, per ciascun prodotto, lo score medio ottenuto in ciascuno degli anni compresi tra il 2003 e il 2012, indicando ProductId seguito da tutti gli score medi ottenuti negli anni dell’intervallo. Il risultato deve essere ordinato in base al ProductId.

* [3.](/src/main/java/job3/AmazonFoodAnalytic.java) Un job in grado di generare coppie di prodotti che hanno almeno un utente in comune, ovvero che sono stati recensiti da uno stesso utente, indicando, per ciascuna coppia, il numero di utenti in comune. Il risultato deve essere ordinato in base allo ProductId del primo elemento della coppia e, possibilmente, non deve presentare duplicati.

## RISULTATI 

Sono riportati i primi risultati di ciascuno dei job sopra descritto per un confronto veloce,
e per ognuno i tempi d'esecuzione degli stessi rispettivamente in:

* [JOB1](/src/main/resources/job1_result.txt) File completo.
* [JOB2](/src/main/resources/job2_result.txt) Primi 32K del file.
* [JOB3](/src/main/resources/job3_result.txt) Primi 32K del file.

## ESECUZIONE

1. Una volta scaricato i repository eseguire `gradle build` per scaricare le dipendenze del progetto
2. Creare i jar relativi ai job col comando `gradle fatJar1`, `gradle fatJar2` e `gradle fatJar3`
2. Lanciare Hadoop e copiare su hdfs all'interno della cartella input il csv del dataset scompattato
3. Creare la cartella output in hadoop dove allocare il file risultato
4. Lanciare il comando seguente per eseguire un qualsiasi job del progetto, mettendo in numero del job di riferimendo al posto del simbolo ` * `  

```zsh
$HADOOP_HOME/bin/hadoop jar ~/AmazonFoodAnalytic/build/libs/AmazonFoodAnalytic*-all-1.0.0.jar input/Reviews.csv output/result
```
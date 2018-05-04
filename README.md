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

Il progetto in JAVA contiene rispettivamente in MapReduce:

[1.](/src/main/java/job1/AmazonFoodAnalytic) Un job che sia in grado di generare, per ciascun anno, le dieci parole che sono state più usate nelle recensioni (campo summary) in ordine di frequenza, indicando, per ogni parola, la sua frequenza, ovvero il numero di occorrenze della parola nelle recensioni di quell’anno.
[2.](/src/main/java/job2/AmazonFoodAnalytic) Un job che sia in grado di generare, per ciascun prodotto, lo score medio ottenuto in ciascuno degli anni compresi tra il 2003 e il 2012, indicando ProductId seguito da tutti gli score medi ottenuti negli anni dell’intervallo. Il risultato deve essere ordinato in base al ProductId.
[3.](/src/main/java/job3/AmazonFoodAnalytic) Un job in grado di generare coppie di prodotti che hanno almeno un utente in comune, ovvero che sono stati recensiti da uno stesso utente, indicando, per ciascuna coppia, il numero di utenti in comune. Il risultato deve essere ordinato in base allo ProductId del primo elemento della coppia e, possibilmente, non deve presentare duplicati.

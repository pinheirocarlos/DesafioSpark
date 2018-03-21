# DesafioSpark

Qual o objetivo do comando cache em Spark?
O comando cache() é responsável por enviar um dataset para ser trabalhado em memória.

O mesmo código implementado em Spark é normalmente mais rápido que a implementação equivalente em MapReduce. Por quê?
Spark faz uso da memória, sendo mais rápido em seu processamento, diferentemente do MapReduce, que utiliza motores de busca baseados em disco.

Qual é a função do SparkContext ?
Representa a conexão para um cluster Spark e pode ser utilizado para criação de RDDs no cluster.

Explique com suas palavras o que é Resilient Distributed Datasets (RDD).
Trata-se de um dos principais objetos do modelo de programação Spark, pois possui o mecanismo para processamento dos dados de forma distribuída.

GroupByKey é menos eficiente que reduceByKey em grandes dataset. Por quê?
Isso acontece porque reduceByKey combina a saída com uma chave comum em cada partição antes de fazer o “shuffle” nos dados.

Explique o que o código Scala abaixo faz.
val textFile = sc . textFile ( "hdfs://..." )
val counts = textFile . flatMap ( line => line . split ( " " ))
. map ( word => ( word , 1 ))
. reduceByKey ( _ + _ )
counts . saveAsTextFile ( "hdfs://..." )

Trata-se de um Contador de palavras distintas que busca o arquivo no HDFS e ao final, salva um novo arquivo no HDFS.


Responda as seguintes questões devem ser desenvolvidas em Spark utilizando a sua linguagem de preferência.

Arquivo utilizado: NASA_access_log_Jul95

1. Número de hosts únicos.

81.982

2. O total de erros 404.

10845

3. Os 5 URLs que mais causaram erro 404.

 /pub/winvn/readme.txt                       667
 
 /pub/winvn/release.txt                      547
 
 /history/apollo/apollo-13.html              286
 
 /shuttle/resources/orbiters/atlantis.gif    230
 
 /history/apollo/a-001/a-001-patch-small.gif 230


4. Quantidade de erros 404 por dia.
 
 19/Jul/1995  636
 06/Jul/1995  629
 07/Jul/1995  564
 13/Jul/1995  524
 05/Jul/1995  491
 03/Jul/1995  473
 11/Jul/1995  468
 18/Jul/1995  463
 12/Jul/1995  459
 25/Jul/1995  457
 20/Jul/1995  426
 14/Jul/1995  406
 17/Jul/1995  403
 10/Jul/1995  390
 04/Jul/1995  355
 09/Jul/1995  341
 27/Jul/1995  334
 21/Jul/1995  332
 24/Jul/1995  324
 26/Jul/1995  319
 01/Jul/1995  314
 08/Jul/1995  299
 02/Jul/1995  289
 16/Jul/1995  255
 15/Jul/1995  252
 23/Jul/1995  230
 22/Jul/1995  180
 28/Jul/1995  93

5. O total de bytes retornados.
38.695.973.491


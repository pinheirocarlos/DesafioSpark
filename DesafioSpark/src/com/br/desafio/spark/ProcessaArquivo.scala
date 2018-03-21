package com.br.desafio.spark

import org.apache.spark._;
import org.apache.spark.SparkContext._;
import org.apache.log4j._;
import java.sql.Date

object ProcessaArquivo {
 
   def main(args: Array[String]) {
     
     //serao retornados somente os erros durante o processo, 
     //caso houver
     Logger.getLogger("org").setLevel(Level.ERROR)
 
     
     //indico que o processamento sera local
     //utilizando todo o poder de processamento da minha máquina
     val sc = new SparkContext("local[*]", "ProcessaArquivoSpark")
   
  
     val rows = sc.textFile("file:///E:/Semantix/access_log_Jul95")
     
     //subo conteúdo acima em memoria
     rows.cache()
     println(" === " + rows.count() + " linhas foram carregadas em memória para processamento")
     
     //tratamento de linhas
     val linhas = rows.filter(x => x.length() >= 10);
    
    //Buscara os hosts unicos agrupados
     retornaHostsUnicos(linhas);
    
    analiseErro404(linhas);
    
    
    errosPorDia(linhas);
    
    //retorna qtd de bytes
    totalBytes(linhas);
		
    
    //encerro o processamento
    sc.stop();
    println("\nFim")
     
     
  };
  

   def retornaHostsUnicos(linhas: org.apache.spark.rdd.RDD[String]) {
    //uso padrão  - - para identificar coluna de hosts
     val resultado = linhas.map(x => x.toString().split(" - - ")(0).trim())
     //aplico group by 
     .distinct().groupBy(x => x);
    println("\nHosts Unicos: " + resultado.count());
  };
  
   def analiseErro404(linhas: org.apache.spark.rdd.RDD[String]) {
    val resultado = linhas.map(interpretarUrlCodigo)
      .filter((tupla) => { tupla._2 == 404 })
      .mapValues(x => (x, 1))
      .reduceByKey((x, y) => (x._1, x._2 + y._2))
      .sortBy(_._2._2, false);
    println("\n 05 URLS com incidencia de erro 404: ");
    resultado.take(5).foreach(println);
     
    val colunas = linhas.map(line => {
      val colunas = line.toString().split(" ");
      if (colunas.length > 1) {
        colunas.init.last;
      } else {
        println("Informação inválida");
        println(colunas(0).toString());
      };
    });
    val filtro = colunas.filter(field => field == "404");
    println("\n debug: " + colunas.count());
    println("\n Qtd de erros 404: " + filtro.count());
        
  };
  
   def errosPorDia(linhas: org.apache.spark.rdd.RDD[String]) {
    val resultado = linhas.map(processaCampoData)
      .filter((tupla) => { tupla._2 == 404 })
      .mapValues(x => (x, 1))
      .reduceByKey((x, y) => (x._1, x._2 + y._2))
      .sortBy(_._2._2, false);
    println("\nErros por dia do maior para o menor: ")
    resultado.collect().foreach(println);
  };
  
   def totalBytes(linhas: org.apache.spark.rdd.RDD[String]) {
    val resultado = linhas.map(line => {
      val byte: Long = scala.util.Try(line.split(" ").last.toLong) getOrElse 0;
      byte
    }).reduce((x, y) => x + y);
    println("\nTotal dos dados retornados (bytes): " + resultado)
  };
  
    def interpretarUrlCodigo(linha: String): (String, Int) = {
    val passo1 = linha.split(" - - ")
    var codigo: Int = 999;
    var url: String = "";
    if (passo1.length == 2) {
      val passo2 = passo1(1).split("] \"GET ");
      if (passo2.length == 2) {
        val passo3 = passo2(1).split(" ");
        url = passo3(0).trim();
        codigo = scala.util.Try(passo3(2).trim().toInt) getOrElse 999;
      };
    };
    (url, codigo)
  };
  
  
    def processaCampoData(linha: String): (String, Int) = {
    val passo1 = linha.split(" - - ");
    var codigo: Int = 999;
    var dateStr: String = "SEM_DATA";
    if (passo1.length == 2) {
      val passo2 = passo1(1).split("] \"GET ");
      if (passo2.length == 2) {
        val passo3 = passo2(1).split(" ");
        dateStr = passo2(0).trim().substring(1, 12);
        codigo = scala.util.Try(passo3(2).trim().toInt) getOrElse 999;
      };
    };
    (dateStr, codigo)
  };
  
}
package progetto;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.AbstractJavaRDDLike;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;

import scala.Tuple2;


public class CliqueCounter {


	public static void main(String[] args) {
		
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);
		
		SparkConf sc = new SparkConf();
		sc.setAppName("CliqueCounter");
		sc.setMaster("local[*]");
		
		JavaSparkContext jsc = new JavaSparkContext(sc);		

		JavaRDD<String> rawData = jsc.textFile("data/Gowalla_edges.csv");
//		Link al dataset: http://snap.stanford.edu/data/loc-Gowalla.html
//		Grafo direzionato

		
//		PARSING, PULIZIA E CREAZIONE STRUTTURE DATI
		
//		Estraiamo gli archi in tuple <n1, n2>
		JavaPairRDD<Integer, Integer> edges = rawData.mapToPair(l -> new Tuple2<Integer, Integer>(Integer.parseInt(l.split("	")[0]),
				Integer.parseInt(l.split("	")[1])));
		
//		Contiamo il grado di ogni nodo, che esprime il numero di nodi con i quali è connesso tramite un arco.
		JavaPairRDD<Integer, Integer> cards = edges.mapToPair(l -> new Tuple2<Integer, Integer>(l._1, 1));
//		<nodo, grado>
        JavaPairRDD<Integer, Integer> cards1 = cards.reduceByKey((x,y)->x+y);


//      Facciamo join tra gli archi espressi sottoforma di <n1, n2> e i gradi dei nodi espressi come <n1, g1>
//      Otteniamo <n1; (n2, c1)>
		JavaPairRDD<Integer, Tuple2<Integer,Integer>> coppie_temp1 = edges.join(cards1);
		

//		Scambiamo le chiavi...
//		<n2; (n1, c1)>
		coppie_temp1 = coppie_temp1.mapToPair(l->new Tuple2<Integer, Tuple2<Integer,Integer>>(l._2._1 , new Tuple2(l._1 , l._2._2)));
		
//		... e rifacciamo il join con i gradi sulla chiave attuale
//		<n2; ((n1, c1), c2)>
		JavaPairRDD<Integer,Tuple2<Tuple2<Integer,Integer>,Integer>> coppie_temp2 = coppie_temp1.join(cards1);
		
//		Adesso scriviamo un metodo per aggiustare le coppie in modo tale da ottenere <(n1,g1);(n2,g2)> 
		JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> nodeDegrees = coppie_temp2.mapToPair(new AggiustaCoppie());
		
//		Questa RDD contiene tutte le informazioni a noi necessarie per l'algoritmo, avendo sia la lista degli archi
//		che le cardinalità di ciscun nodo, in ciascuna riga.
//		Inoltre da ora in avanti ogni nodo sarà espresso da una tupla u = (n1,g1), dove n1 è l'etichetta del nodo u, mentre c1 il suo grado
		
//		Possiamo inziare con l'applicazione dell'algoritmo
		
		long startTime = System.currentTimeMillis();
		
		
//      _____INIZIO ROUND 1_____
		
//		Filtriamo gli archi secondo l'ordinamento totale descritto dal paper due condizioni:
//		1. Archi il cui nodo di arrivo ha grado maggiore di quello di partenza
//		2. Archi che hanno nodi con grado uguale ma label del secondo nodo maggiore rispetto alla label del primo
		JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> nodeDegrees1 = nodeDegrees.filter(l->l._1._2<l._2._2);
		JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> nodeDegrees2 = nodeDegrees.filter(l->(l._1._2.equals(l._2._2)) && (l._1._1<l._2._1));

//		Uniamo le due strutture e otteniamo il primo risultato Map1: archi <u; v> tali per cui u < v
		JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> map1 = nodeDegrees1.union(nodeDegrees2);
		
//		Reduce1: raggruppiamo il precedente risultato per chiave ottendendo Gamma^+(u), ovvero lista dei nodi adiacenti a u tali che u < v
//		con la seguente struttura <u; {x1, x2, ... ,xn}>
		JavaPairRDD<Tuple2<Integer,Integer>, Iterable<Tuple2<Integer,Integer>>> reduce1 = map1.groupByKey().filter(l->size(l._2)>=2);
		
//      _____FINE ROUND 1_______
		
		
		
		
//      _____INIZIO ROUND 2_____
		
//		Facciamo il map sugli archi restituendo <(u,v),"$"> se u < v, altrimenti <(u,v),"0">
		JavaPairRDD<Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>,String> map2_1 = nodeDegrees.mapToPair(new Dollar());
       
//		Adesso prendiamo l'output del primo reduce nella forma <u; {x1, x2, ... ,xn}> dove le x rappresentano i vicini di u con grado maggiore
//		ed estraiamo le differenti coppie. Vogliamo quindi ottenere:
//		<(x1, x2); u>
//		<(x2, x3); u>
//		<(x1, x3); u> ecc..
//		Scriviamo un metodo per farlo
		JavaPairRDD<Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>, Tuple2<Integer, Integer>> coppie = 
				reduce1.flatMapToPair(new EstraiCoppiePlus());
		
//		Map2: adesso raggruppiamo per chiave i nodi u che formano G^+(u). Partendo da:
//		<(x1, x2); u1>, <(x1, x2); u2> ecc... , raggruppiamo in un'unica riga
//		<(x1, x2); {u1, u2, ... , un}>
		JavaPairRDD<Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>, Iterable<Tuple2<Integer, Integer>>> coppiePlus = coppie.groupByKey();
		
//		Etichettiamo con "$" le coppie i cui nodi costituiscono un arco nel grafo
//		<(xi,xj),{u1,...uk}> ---> <(xi,xj),{u1,...,uk} U $}> se e solo se (xi,xj) è effettivamente un arco
		JavaPairRDD<Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>, Tuple2<Iterable<Tuple2<Integer, Integer>>, String>> map2 =
				coppiePlus.join(map2_1);
		
//		Reduce2: scartiamo le coppie del Map2 che non sono archi
		JavaPairRDD<Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>, Iterable<Tuple2<Integer, Integer>>> reduce2
		        = map2.mapToPair
		        (l->new Tuple2<Tuple2<Tuple2<Integer,Integer>,Tuple2<Integer,Integer>>,Iterable<Tuple2<Integer,Integer>>>(l._1,l._2._1));
      
//      _____FINE ROUND 2_______
		
		
		
		
//      _____INIZIO ROUND 3_____
		
//		Map3: Partendo da <(x1,x2); {u1, ... ,uk}> estraiamo le singole u in chiave con l'arco di riferimento in valore
//		<u1; (x1,x2)>
//		<u2; (x1,x2)>
//		...
//		<uh; (x1,x2)>
//		...
//		<uk; (x1,x2)>
		JavaPairRDD<Tuple2<Integer, Integer>, Iterable<Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>>> map3 = 
				reduce2.flatMapToPair(new terzoMap()).groupByKey();
		
//		Reduce3: Raggruppo tutti i nodi appartenenti a G^+(u) (u chiave)
//		Conterò dunque il numero di archi in tali sottografi ottenendo il numero di triangoli per ricorsione
		
		JavaPairRDD<Tuple2<Integer,Integer>,Integer> reduce3 = map3.mapToPair(l->new Tuple2(l._1,size(l._2)));

		
//		Contiamo i triangoli:
		
		Integer cliqueCount = reduce3.map(l->l._2).reduce((x,y)->x+y);
		System.out.println(cliqueCount);
		
//      _____FINE ROUND 3_______
		
		long estimatedTime = System.currentTimeMillis() - startTime;
		double minutes = (double)estimatedTime/6000;
		System.out.println("Tempo impiegato (millisecondi): " + estimatedTime);
		System.out.println("Tempo impiegato (minuti): " + minutes);

        
//      ______________________________________IMPLEMENTAZIONE DATABASE SU NEO4J_______________________________________________
//        
//		/*_____CONNESSIONE________*/
//
//
//		String uri = "bolt://localhost:7687";
//		AuthToken token = AuthTokens.basic("neo4j", "123456");
//		Driver driver = GraphDatabase.driver(uri, token);
//		Session s = driver.session();
//		
//		/*CREAZIONE NODI IN NEO4J             		 */
//		List<Tuple2<Integer, Integer>> nodes = cards1.collect();
//		
//		for(Tuple2<Integer, Integer> l : nodes) {
//			int p1 = l._1;
//			int p2 = l._2;
//			
//			String cql1 = "create (m {nodo: "+p1+", grado: "+p2+"})";
//			s.run(cql1);
//		}
//		
//		
//		/*CREAZIONE ARCHI IN NEO4J             			*/
//		List<Tuple2<Integer, Integer>> archi = edges.collect();
//		for(Tuple2<Integer, Integer> l : archi) {
//		int p1 = l._1;
//		int p2 = l._2;
//		
//		String cql2 = "match (n),(m) "
//				+ "where n.nodo = "+p1+" and m.nodo = "+p2+" "
//				+ "create (n)-[a:FriendsWith]->(m) ";
//		s.run(cql2);
//		}
//		
//		/*SELEZIONE SOTTOGRAFI DEI TRIANGOLI IN NEO4J	*/
//		int num = 1;
//		List<Tuple2<Tuple2<Integer, Integer>, Iterable<Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>>>> sottografi = map3.take(num);
//		
//		ArrayList<Integer> nodicoll = new ArrayList<Integer>();
//		
//		for(Tuple2<Tuple2<Integer, Integer>, Iterable<Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>>> l : sottografi) {
//			
//			int p1 = l._1._1;
//			Iterator<Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>> it = l._2.iterator();
//
//				while(it.hasNext()) {
//					Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> t = it.next();
//					int p2 = t._1._1;
//					int p3 = t._2._1;
//					
//					String cql5 ="match (n),(m) "
//							+ "where n.nodo = "+p2+" and m.nodo = "+p3+" "
//							+ "create (n)-[r:FriendsWith]->(m)";
//					s.run(cql5);
//					System.out.println("Collego il nodo "+p2+" al nodo "+p3);
//
//					if (!(nodicoll.contains(p2))) {
//						nodicoll.add(p2);	
//						String cql3 = "match (n),(m) "
//									+ "where n.nodo = "+p1+" and m.nodo = "+p2+" "
//									+ "create (n)-[r:FriendsWith]->(m)";
//						s.run(cql3);
//						
//					}
//					if (!(nodicoll.contains(p3))) {
//						nodicoll.add(p3);
//						String cql4 = "match (n),(m) "
//									+ "where n.nodo = "+p1+" and m.nodo = "+p3+" "
//									+ "create (n)-[r:FriendsWith]->(m)";
//						s.run(cql4);
//					}
//
//				}
//			
//			System.out.println(p1);
//		}
        
        
        
        
	}

	
//	Metodo per calcolare la lunghezza di una Lista (iterable)
//	utilizzando il metodo size di Collection (https://docs.oracle.com/javase/8/docs/api/java/util/Collection.html)
	public static int size(Iterable<?> data) {
	    if (data instanceof Collection) {
	        return ((Collection<?>) data).size();
	    }
	    int counter = 0;
	    for (Object i : data) {
	        counter++;
	    }
	    return counter;
	}
}


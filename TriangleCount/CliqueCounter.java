//https://spark.apache.org/docs/2.2.0/api/java/org/apache/spark/graphx/Graph.html
package progetto;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
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
		
		//Inizio del calcolo del tempo di esecuzione
		long startTime = System.currentTimeMillis();   
		
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);
		
		SparkConf sc = new SparkConf();
		sc.setAppName("CliqueCounter");
		sc.setMaster("local[*]");
		
		JavaSparkContext jsc = new JavaSparkContext(sc);		

		JavaRDD<String> rawData = jsc.textFile("data/Gowalla_edges.csv");
        
		//parsing e pulizia dei dati
		JavaPairRDD<Integer, Integer> Edges = rawData.mapToPair(l -> new Tuple2<Integer, Integer>(Integer.parseInt(l.split("	")[0]),
				Integer.parseInt(l.split("	")[1])));
		
        /////////INIZIO ROUND 1
		JavaPairRDD<Integer, Integer> Cards = Edges.mapToPair(l -> new Tuple2<Integer, Integer>(l._1, 1));
        JavaPairRDD<Integer, Integer> Cards1 = Cards.reduceByKey((x,y)->x+y);
        
        //Facciamo join usando il grado del nodo come valore.
        // (n1, (n2, c1))
		JavaPairRDD<Integer, Tuple2<Integer,Integer>> EdgesDegree = Edges.join(Cards1);
		
		// (n2, (n1, c1)
		//Scambiamo le chiavi e rifacciamo join sulla chiave attuale
		EdgesDegree=EdgesDegree.mapToPair(l->new Tuple2<Integer, Tuple2<Integer,Integer>>(l._2._1 , new Tuple2(l._1 , l._2._2)));
		
		// (n2, (n1, c1) , c2)
		JavaPairRDD<Integer,Tuple2<Tuple2<Integer,Integer>,Integer>> WTF1 = EdgesDegree.join(Cards1);
		
		//Adesso scriviamo un metodo per aggiustare le coppie in modo tale che si abbia ((n1,c1),(n2,c2)) 
		JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> NodeDegrees = WTF1.mapToPair(new AggiustaCoppie());
		
		//filtriamo sugli archi il cui primo nodo ha grado minore del secondo o che hanno gradi uguali 
		//ma la label del primo nodo è minore della label del secondo
		JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> NodeDegrees1=NodeDegrees.filter(l->l._1._2<l._2._2);
		JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> NodeDegrees2=NodeDegrees.filter(l->(l._1._2.equals(l._2._2)) && (l._1._1<l._2._1));

		//Map1: archi tali per cui u<v
		JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> Map1 = NodeDegrees1.union(NodeDegrees2);
		
		//Reduce1: raggruppiamo per chiave ottende Gamma^+(u), lista dei nodi adiacenti a u tali che u<v
		// [(u), (x1,x2,...,xn)]
		JavaPairRDD<Tuple2<Integer,Integer>, Iterable<Tuple2<Integer,Integer>>> Reduce1 = Map1.groupByKey().filter(l->size(l._2)>=2); //primo reduce
		
		/////////FINE ROUND 1
		
        /////////INIZIO ROUND 2
		// mettere come chiave ("u,v") stringa
		//Facciamo il map sul grafo con struttura (n1,grado(n1)),(n2,grado(n2)) restituendo <(u,v),"$"> se u<v
		//(u,v) arco etichettato con i gradi: Idea, creare classe Nodo con costruttore label e grado
		JavaPairRDD<Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>,String> Map2_1 = NodeDegrees.mapToPair(new Dollar());
       
		JavaPairRDD<Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>, Tuple2<Integer, Integer>> Coppie = 
				Reduce1.flatMapToPair(new EstraiCoppiePlus());
		
		//Map2: raggruppo per chiave i nodi u che formano G^+(u)
		JavaPairRDD<Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>, Iterable<Tuple2<Integer, Integer>>> CoppiePlus=Coppie.groupByKey();
		JavaPairRDD<Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>, Tuple2<Iterable<Tuple2<Integer, Integer>>, String>> Map2=
				CoppiePlus.join(Map2_1);
		
		//Reduce2: (xi,xj),{u1,...,uk} U $} --> (xi,xj),{u1,...uk} (ovvero controllo se (xi,xj) è effettivamente un arco
		JavaPairRDD<Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>, Iterable<Tuple2<Integer, Integer>>> Reduce2
		        = Map2.mapToPair
		        (l->new Tuple2<Tuple2<Tuple2<Integer,Integer>,Tuple2<Integer,Integer>>,Iterable<Tuple2<Integer,Integer>>>(l._1,l._2._1));
      
		//FINE ROUND 2
		
		//INIZIO ROUND 3
		
		//Map3: [(x1,x2),(u1,...,uk)] ---> [uh,(x1,x2)]
		JavaPairRDD<Tuple2<Integer, Integer>, Iterable<Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>>> Map3 = 
				Reduce2.flatMapToPair(new terzoMap()).groupByKey();
		
		//Reduce3 (ultimo) : raggruppo tutti i nodi appartenenti a G^+(u) (u chiave)
		//Conterò dunque il numero di archi in tali sottografi ottenendo il numero di triangoli per ricorsione
		
		JavaPairRDD<Tuple2<Integer,Integer>,Integer> Reduce3 = Map3.mapToPair(l->new Tuple2(l._1,size(l._2)));
		
		//FINE ROUND 3
		
		//Contiamo i triangoli:
		
		Integer CliqueCounts = Reduce3.map(l->l._2).reduce((x,y)->x+y);
		System.out.println(CliqueCounts);
		
		//Infine vediamo il tempo di esecuzione dell'algoritmo (in minuti)
		long estimatedTime = System.currentTimeMillis() - startTime;
		double minutes = TimeUnit.MILLISECONDS.toMinutes(estimatedTime);
		System.out.println("\n Tempo trascorso: " + minutes);
		
        //Neo4J
		String uri = "bolt://localhost:7687";
		AuthToken token = AuthTokens.basic("neo4j", "123456");
		Driver driver = GraphDatabase.driver(uri, token);
		Session s = driver.session();

	}

	//Metodo per calcolare la lunghezza di una Lista (iterable)
	//utilizzando il metodo size di Collection (https://docs.oracle.com/javase/8/docs/api/java/util/Collection.html)
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

//Metodo scartato (troppo specifico) per calcolare la dimensione di liste.
	//private static int size(Iterable<Tuple2<Integer,Integer>> l) {
		//Iterator<Tuple2<Integer,Integer>> it = l.iterator();
		//int sum=0;
		//while (it.hasNext()) {
			//  it.next();
			  //sum++;
		//	}
		//return sum;
	//}

//}


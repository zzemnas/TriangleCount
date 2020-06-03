package progetto;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class AggiustaCoppie implements PairFunction<Tuple2<Integer,Tuple2<Tuple2<Integer,Integer>,Integer>>, Tuple2<Integer,Integer>, Tuple2<Integer,Integer>> {

	//(n2, (n1, c1) , c2)	
	public Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> call(
			Tuple2<Integer, Tuple2<Tuple2<Integer, Integer>, Integer>> l) throws Exception {
		    Tuple2<Integer,Integer> e1 = new Tuple2<Integer,Integer>(l._2._1._1,l._2._1._2);
		    Tuple2<Integer,Integer> e2 = new Tuple2<Integer,Integer>(l._1,l._2._2);
		    return new Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>(e1,e2);
	}
	
}

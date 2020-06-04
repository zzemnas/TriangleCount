package progetto;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;

public class terzoMap implements PairFlatMapFunction<Tuple2<Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>, Iterable<Tuple2<Integer, Integer>>>,
	Tuple2<Integer, Integer>, Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>> {

	@Override
	public Iterator<Tuple2<Tuple2<Integer, Integer>, Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>>> call(
			Tuple2<Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>, Iterable<Tuple2<Integer, Integer>>> l)
			throws Exception {
	
		// Coppia di nodi Xi, Xj
		Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> x = new Tuple2<Tuple2<Integer, Integer>,Tuple2<Integer, Integer>>(l._1._1,l._1._2);
	

		List<Tuple2<Tuple2<Integer, Integer>, Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>>> output = 
				new ArrayList<Tuple2<Tuple2<Integer, Integer>, Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>>>();
		
		
		for (Tuple2<Integer,Integer> u : l._2) {
			output.add(new Tuple2<Tuple2<Integer, Integer>, Tuple2<Tuple2<Integer,Integer>,Tuple2<Integer, Integer>>>(u, x));
		}
		
		return output.iterator();
	}



}

package progetto;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;

public class EstraiCoppiePlus implements PairFlatMapFunction<Tuple2<Tuple2<Integer,Integer>, Iterable<Tuple2<Integer,Integer>>>, 
 Tuple2<Tuple2<Integer,Integer>,Tuple2<Integer,Integer>>, Tuple2<Integer,Integer>> {

	@Override
	public Iterator<Tuple2<Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>, Tuple2<Integer, Integer>>> call(
			Tuple2<Tuple2<Integer, Integer>, Iterable<Tuple2<Integer, Integer>>> l) throws Exception {
		
		//nodo su cui calcolare Gamma^+(u)
		Tuple2<Integer,Integer> u = new Tuple2<Integer,Integer>(l._1._1,l._1._2);
		
		//lista che dà le coppie in relazione <
		List<Tuple2<Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>, Tuple2<Integer, Integer>>> output = 
				new ArrayList<Tuple2<Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>, Tuple2<Integer, Integer>>> ();
		
		
		for (Tuple2<Integer,Integer> p1:l._2) {
			
			for (Tuple2<Integer,Integer> p2:l._2){
				
				if (p1._1.compareTo(p2._1)!=0) {
					
					if ((p1._2<p2._2)||(p1._2.equals(p2._2) && p1._1<p2._1)) {
					Tuple2<Tuple2<Integer,Integer>,Tuple2<Integer,Integer>> s = new Tuple2<Tuple2<Integer,Integer>,Tuple2<Integer,Integer>>(p1,p2);
					output.add(new Tuple2<Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>, Tuple2<Integer, Integer>>(s,u));
					}
					}
			}
		}
		
		return output.iterator();
		
	}

}

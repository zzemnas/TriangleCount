package progetto;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class Dollar implements PairFunction<Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>, 
 Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>,String> {

	@Override
	public Tuple2<Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>, String> call(
			Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> l) throws Exception {
		if((l._1._2<l._2._2) || (l._1._2.equals(l._2._2) && l._1._1<l._2._1)) {
			return new Tuple2<Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>, String>(l,"$");
		}
		return new Tuple2<Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>, String>(l,"0");

		
	}

}

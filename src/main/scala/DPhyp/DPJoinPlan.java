package DPhyp;

import java.io.IOException;
import java.util.List;


public abstract class DPJoinPlan implements Comparable<DPJoinPlan>{
	public DPJoinPlan p1, p2;
	
	public abstract String print(String indent);
	public abstract Double getCost();
	public abstract double getStatistics(Integer joinVar) throws IOException;
	public abstract void computeCost() throws IOException;
	public abstract List<Integer> getOrdering();
}

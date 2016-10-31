package DPhyp;

import java.io.IOException;
import java.util.List;
import java.util.Set;

public class Move extends DPJoinPlan{
	private Double cost;
	private String table, location;
	
	public Move(DPJoinPlan p1, String location) {
		this.p1=p1;
		this.p2=null;
		this.location=location;
		cost=p1.getCost()+100;
	}
	
	public void explain(){

	}

	/*Override
	/public String print() {
		return "join on: "+vars+", ["+p1.print() +", "+p2.print()+"]";
	}*/
	@Override
	public String print(String indent) {
		return String.format("%s#--Move to %s cost %.2f\n%s",indent,location,cost, p1.print(indent+"\t") );
	}

	@Override
	public int compareTo(DPJoinPlan o) {
		return cost.compareTo(o.getCost());
	}
	@Override
	public Double getCost() {
		return cost;
	}

	@Override
	public void computeCost() throws IOException {
	}

	@Override
	public List<Integer> getOrdering() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public double getStatistics(Integer joinVar) throws IOException {
		// TODO Auto-generated method stub
		return 45;
	}

}

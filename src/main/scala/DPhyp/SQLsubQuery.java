package DPhyp;

import java.io.IOException;
import java.util.List;
import java.util.Set;

public class SQLsubQuery extends DPJoinPlan{
	private Double cost;
	public boolean isJoin;
	private String table, location;
	Set<Integer> vars;
	
	public SQLsubQuery(DPJoinPlan p1, DPJoinPlan p2, Set<Integer>  vars, String location) {
		isJoin=true;
		this.p1=p1;
		this.p2=p2;
		this.vars=vars;
		this.location=location;
		cost=p1.getCost()+p2.getCost()+50;
	}

	public SQLsubQuery(String table, String location) {
		isJoin=false;
		this.table=table;
		this.location=location;
		cost=10.0;
	}
	
	public void explain(){

	}

	/*Override
	/public String print() {
		return "join on: "+vars+", ["+p1.print() +", "+p2.print()+"]";
	}*/
	@Override
	public String print(String indent) {
		if(!isJoin){
			return String.format("%s#--Scan %s location %s cost %.2f",indent,table,location,cost);
		}
		else{
			return String.format("%s#--Join %s location %s cost %.2f\n%s\n%s",indent, vars, location, cost, p1.print(indent+"\t"), p2.print(indent+"\t"));
		}
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

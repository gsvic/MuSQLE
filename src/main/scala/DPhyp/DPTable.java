package DPhyp;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

public class DPTable {
	private HashMap<BitSet,HashMap<String, DPJoinPlan>>dptable;
	public List<String> engines;
	
	public DPTable(List<String> engines) {
		dptable= new HashMap<BitSet, HashMap<String, DPJoinPlan>>();
		this.engines=engines;
	}
	
	public DPJoinPlan getOptimalPlan(String engine, BitSet b){
		HashMap<String, DPJoinPlan> v = dptable.get(b);
		if(v==null)
			return null;
		return v.get(engine);
	}	
	
	public DPJoinPlan getOptimalPlan(BitSet b){
		HashMap<String, DPJoinPlan> v = dptable.get(b);
		if(v==null)
			return null;
		DPJoinPlan ret=null;
		for(Entry<String, DPJoinPlan> e : v.entrySet()){
			if(ret==null)
				ret=e.getValue();
			else if(ret.getCost()>e.getValue().getCost())
				ret=e.getValue();
		}
		return ret;
	}
	
	
	public HashMap<String, DPJoinPlan> getAllPlans(BitSet b){
		HashMap<String, DPJoinPlan> v = dptable.get(b);
		if(v==null)
			return null;
		return v;
	}
	
	
	public void checkAndPut(String engine, BitSet b, DPJoinPlan plan){
		HashMap<String, DPJoinPlan> v = dptable.get(b);
		if(v==null){
			v = new HashMap<String, DPJoinPlan>();
			v.put(engine, plan);
			dptable.put(b, v);
			return;
		}
		DPJoinPlan v1 = v.get(engine);
		if(v1==null){
			v.put(engine, plan);
			dptable.put(b, v);
			return;
		}
		if(v1.getCost()> plan.getCost()){
			v.put(engine, plan);
		}
	}
	
	public void print(){
		System.out.println(dptable);
	}
}

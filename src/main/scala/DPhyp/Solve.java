package DPhyp;

import java.util.*;
import java.util.Map.Entry;

//DPccp

public class Solve {
	private int n,totalChecks, cacheChecks;
	private static DPTable dptable;
	public int maxCoverage;
	public TreeMap<Integer, TreeMap<Integer, BitSet>> edgeGraph;
	public HashMap<Integer,List<String>> location;
	
	
	public static void main(String[] args) {
		Solve s = new Solve();
		s.genGraph();
		System.out.println(s.edgeGraph);
		List<String> engines = new ArrayList<String>();
		engines.add("postgres");
		engines.add("spark");
		dptable = new DPTable(engines);
		
		s.solve();

		System.out.println("Checked: "+s.totalChecks);
	}

	private void genGraph(){
		this.edgeGraph = new TreeMap<Integer, TreeMap<Integer, BitSet>>();

		TreeMap<Integer, BitSet> t = new TreeMap<Integer, BitSet>();
		BitSet b = new BitSet(5);
		b.set(2);
		t.put(1, b);
		edgeGraph.put(1, t);

		t = new TreeMap<Integer, BitSet>();
		b = new BitSet(5);
		b.set(3);
		b.set(4);
		t.put(2, b);
		b = new BitSet(5);
		b.set(1);
		t.put(1, b);
		edgeGraph.put(2, t);

		t = new TreeMap<Integer, BitSet>();
		b = new BitSet(5);
		b.set(2);
		b.set(4);
		t.put(2, b);
		b = new BitSet(5);
		b.set(5);
		t.put(3, b);
		edgeGraph.put(3, t);


		t = new TreeMap<Integer, BitSet>();
		b = new BitSet(5);
		b.set(2);
		b.set(3);
		t.put(2, b);
		b = new BitSet(5);
		b.set(5);
		t.put(3, b);
		edgeGraph.put(4, t);

		t = new TreeMap<Integer, BitSet>();
		b = new BitSet(5);
		b.set(3);
		b.set(4);
		t.put(3, b);
		edgeGraph.put(5, t);
		n=5;
		
		location=new HashMap<Integer,List<String>>();
		ArrayList<String> l = new ArrayList<String>();
		l.add("postgres");
		location.put(1, l);
		location.put(2, l);
		location.put(3, l);

		ArrayList<String> l1 = new ArrayList<String>();
		l1.add("postgres");
		l1.add("spark");
		location.put(4, l1);
		
		ArrayList<String> l2 = new ArrayList<String>();
		l2.add("spark");
		location.put(5, l2);
	}

	private void solve() {
		for(Integer v : edgeGraph.descendingKeySet()){
			BitSet b = new BitSet(n);
			b.set(v);
			for(String engine : location.get(v)){
				dptable.checkAndPut(engine, b, new SQLsubQuery(v+"", engine));
			}
		}
		
		for(Integer v : edgeGraph.descendingKeySet()){
			BitSet b = new BitSet(n);
			b.set(v);
			emitCsg(b);
			BitSet bv = new BitSet(n);
			for (int i = 1; i <= v; i++) {
				bv.set(i);
			}
			enumerateCsgRec(b,bv);
		}
		BitSet b = new BitSet(n);
		for (int i = 1; i <= n; i++) {
			b.set(i);
		}
		System.out.println("Optimal:\n"+ dptable.getOptimalPlan(b).print(""));
	}

	private void enumerateCsgRec(BitSet b, BitSet bv) {
		//System.out.println("EnumerateCsgRec S1: "+b+" X: "+bv);
		BitSet N = neighboor(b,bv);
		//System.out.println("N: "+N);
		if(N.isEmpty())
			return;
		PowerSet pset = new PowerSet(N);
        for(BitSet t:pset)
        {
        	if(!t.isEmpty()){
        		t.or(b);
        		//System.out.println("Check DPtable: "+t);
        		if(dptable.getAllPlans(t)!=null){
            		emitCsg(t);
        		}
        	}
        }
		
        pset = new PowerSet(N);
        for(BitSet t:pset)
        {
        	if(!t.isEmpty()){
        		t.or(b);
        		BitSet Xnew = new BitSet(n);
        		Xnew.or(bv);
        		Xnew.or(N);
        		enumerateCsgRec(t,Xnew);
        	}
        }
		
		
		
	}



	private void emitCsg(BitSet s1) {
		//System.out.println("EmitCsg S1: "+s1);
		BitSet X = new BitSet(n);
		int mins1=s1.nextSetBit(0);
		for (int i = 1; i <= mins1; i++) {
			X.set(i);
		}
		X.or(s1);
		//System.out.println("X: "+ X);
		BitSet N = neighboor(s1,X);
		//System.out.println("N: "+N);
		for (int i = N.size(); i >=1 ; i--) {
			BitSet s2 = new BitSet(n);
			if(N.get(i)){
        		s2.set(i);
            	//removed check for connectedness
        		emitCsgCmp(s1,s2);
        		enumerateCmpRec(s1,s2,X);
			}
		}
	}
	
	private void enumerateCmpRec(BitSet s1, BitSet s2, BitSet X) {
		BitSet N = neighboor(s2,X);
		if(N.isEmpty())
			return;
		PowerSet pset = new PowerSet(N);
        for(BitSet t:pset)
        {
        	if(!t.isEmpty()){
        		t.or(s2);
        		//System.out.println("Check DPtable: "+t);
        		if(dptable.getAllPlans(t)!=null){
            		emitCsgCmp(s1,t);
        		}
        	}
        }
        
        X.or(N);
        N = neighboor(s2,X);
		if(N.isEmpty())
			return;
		pset = new PowerSet(N);
        for(BitSet t:pset)
        {
        	if(!t.isEmpty()){
        		t.or(s2);
        		enumerateCmpRec(s1,t,X);
        	}
        }
	}

	private void emitCsgCmp(BitSet s1, BitSet s2) {
		System.out.println("EmitCsgCmp s1:"+s1+" s2: "+s2);
		Set<Integer> vars = findJoinVars(s1,s2);
		totalChecks++;
		BitSet s = new BitSet(n);
		s.or(s1);
		s.or(s2);
		for(Entry<String, DPJoinPlan> e1: dptable.getAllPlans(s1).entrySet()){
			for(Entry<String, DPJoinPlan> e2: dptable.getAllPlans(s2).entrySet()){
//				System.out.println("checking: " +e1.getValue().print(""));
//				System.out.println("with: " +e2.getValue().print(""));
				if(e1.getKey().equals(e2.getKey())){
					//no move
					DPJoinPlan r = new SQLsubQuery(e1.getValue(), e2.getValue(), vars, e1.getKey());
					dptable.checkAndPut(e1.getKey(), s, r);
				}
				else{
					//move left to right
					DPJoinPlan m = new Move(e1.getValue(), e2.getKey());
					DPJoinPlan r = new SQLsubQuery(m, e2.getValue(), vars, e1.getKey());
					dptable.checkAndPut(e1.getKey(), s, r);
					
					
					//move right to left
					m = new Move(e2.getValue(), e1.getKey());
					r = new SQLsubQuery(e1.getValue(), m, vars, e2.getKey());
					dptable.checkAndPut(e1.getKey(), s, r);
					
					
					//move all to other engine
					for(String engine : dptable.engines){
						DPJoinPlan m1 = e1.getValue();
						DPJoinPlan m2 = e2.getValue();
						if(!e1.getKey().equals(engine)){
							m1 = new Move(m1, engine);
						}
						if(!e2.getKey().equals(engine)){
							m2 = new Move(m2, engine);
						}
						r = new SQLsubQuery(m1, m2, vars, engine);
						dptable.checkAndPut(engine, s, r);
						
					}
					
				}
			}
			
			//check for grouping filters on subgraphs
		}
		
	}

	private Set<Integer> findJoinVars(BitSet s1, BitSet s2) {
		Set<Integer> ret = new HashSet<Integer>();
		for (int i = 1; i <= n; i++) {
			if(s1.get(i)){
				for(Entry<Integer, BitSet> s : edgeGraph.get(i).entrySet()){
					if(s.getValue().intersects(s2)){
						ret.add(s.getKey());
					}
				}
				
			}
		}
		return ret;
	}

	public BitSet neighboor(BitSet S, BitSet X) {
		BitSet N = new BitSet(n);
		for (int i = 1; i <= n; i++) {
			if(S.get(i)){
				for(Entry<Integer, BitSet> s : edgeGraph.get(i).entrySet()){
					N.or(s.getValue());
				}
			}
		}
		N.andNot(X);
		return N;
	}
}

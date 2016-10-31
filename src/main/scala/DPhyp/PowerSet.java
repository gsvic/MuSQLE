package DPhyp;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Iterator;

public class PowerSet implements Iterator<BitSet>,Iterable<BitSet>{
    private ArrayList<Integer> arr = null;
    private BitSet bset = null;

    @SuppressWarnings("unchecked")
    public PowerSet(BitSet set)
    {
    	arr = new ArrayList<Integer>();
        for (int i = 0; i <= set.size(); i++) {
        	if(set.get(i))
        		arr.add(i);
		}
        
        bset = new BitSet(arr.size() + 1);
    }

    @Override
    public boolean hasNext() {
        return !bset.get(arr.size());
    }

    @Override
    public BitSet next() {
    	BitSet returnSet = new BitSet();
        for(int i = 0; i < arr.size(); i++)
        {
            if(bset.get(i))
                returnSet.set(arr.get(i));
        }
        //increment bset
        for(int i = 0; i < bset.size(); i++) 
        {
            if(!bset.get(i))
            {
                bset.set(i);
                break;
            }else
                bset.clear(i);
        }

        return returnSet;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Not Supported!");
    }

    @Override
    public Iterator<BitSet> iterator() {
        return this;
    }
    
    public static void main(String[] args) {
    	long time =System.currentTimeMillis();
    	int n =3;
		BitSet b = new BitSet(n);
		for (int i = 1; i <= n; i++) {
			b.set(i);
		}
		int count =0;
		PowerSet p = new PowerSet(b);
		for(BitSet b1 : p){
			System.out.println(b1);
			count++;
		}
		System.out.println("Count: "+count);
    	long stoptime =System.currentTimeMillis();
    	System.out.println("Time ms: "+(stoptime-time));
	}
}
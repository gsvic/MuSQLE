package gr.cslab.ece.ntua.musqle.plan.hypergraph

import java.util

import scala.util.control.Breaks._

class PowerSet(set: util.BitSet) {
  private final val arr: util.ArrayList[Integer] = new util.ArrayList[Integer]()

  for (i <- Range(0, set.size)){
    if (set.get(i)){arr.add(i)}
  }
  private final val bitSet: util.BitSet = new util.BitSet(arr.size + 1)

  @Override def hasNext: Boolean = !bitSet.get(arr.size)

  @Override def next: util.BitSet = {
    val returnSet = new util.BitSet()
    for(i <- Range(0, arr.size())){
      if(bitSet.get(i))
        returnSet.set(arr.get(i));
    }
    //increment bitSet
    breakable {
      for(i <- Range(0, bitSet.size())) {
        if (!bitSet.get(i)) {
          bitSet.set(i)
          break
        }
        else {
          bitSet.clear(i)
        }
      }
    }

    returnSet
  }

  @Override def remove() {
    throw new Exception("Not Supported!")
  }

  @Override def iterator: PowerSet = this
}

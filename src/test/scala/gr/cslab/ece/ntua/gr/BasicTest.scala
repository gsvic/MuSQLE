package gr.cslab.ece.ntua.gr

import java.util

import org.scalatest._
import java.util.BitSet

import gr.cslab.ece.ntua.musqle.plan.hypergraph.PowerSet
import gr.cslab.ece.ntua.ThisIsMusqle
import org.scalatest.Matchers
/**
  * Created by vic on 10/10/2016.
  */
class BasicTest extends FunSuiteLike{

  test("MuSQLE string matching test"){
    val thisIsMusqle = new ThisIsMusqle()
    assert(thisIsMusqle.myName().equals("This is MuSQLE"))
  }

  test("PowerSet Test #1"){
    val powerSet = {
      val bitSet = new BitSet(2)
      bitSet.set(1)
      bitSet.set(2)

      new PowerSet(bitSet)
    }
    val bitSet = new util.BitSet()

    assert (powerSet.next.equals(bitSet))
    bitSet.set(1)
    assert (powerSet.next.equals(bitSet))
    bitSet.clear()
    bitSet.set(2)
    assert (powerSet.next.equals(bitSet))
    bitSet.set(1)
    assert (powerSet.next.equals(bitSet))
  }
}

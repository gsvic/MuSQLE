package gr.cslab.ece.ntua.musqle.plan.hypergraph

import java.util
import gr.cslab.ece.ntua.musqle.engine._
import scala.collection.JavaConversions._
import scala.collection.mutable.HashSet

abstract class DPhyp(val moveClass: Class[_] = classOf[Move],
                     val scanClass: Class[_] = classOf[Scan],
                     val joinClass: Class[_] = classOf[Join]) {

  protected var queryInfo: QueryInfo
  protected var numberOfVertices: Int = 0
  protected var maxCoverage: Int = 0
  protected final var location: util.HashMap[Int, util.List[Engine]] = new util.HashMap[Int, util.List[Engine]]()
  protected final val edgeGraph: util.TreeMap[Vertex, util.TreeMap[Int, util.BitSet]] =
    new util.TreeMap[Vertex, util.TreeMap[Int, util.BitSet]]

  protected final val dptable: DPTable = new DPTable(Seq(Engine.SPARK, Engine.POSTGRES))
  protected val cacheChecks: Int = 0
  protected var totalChecks: Int = 0
  protected final val vertices: util.TreeMap[Vertex, List[(Int, Seq[Int])]] =
    new util.TreeMap[Vertex, List[(Int, Seq[Int])]]

  def plan(): DPJoinPlan = {
    generateGraph()
    init()
    solve()
  }
  protected def generateGraph()

  /* Graph initialization */
  protected def init(): Unit ={
    var bitSet: util.BitSet = null
    var keyToAdj: util.TreeMap[Int, util.BitSet] = null

    /* For each vertex create a TreeMap with its
     adjacent nodes and the corresponding key */
    for (e <- vertices.entrySet()) {
      keyToAdj = new util.TreeMap[Int, util.BitSet]()
      e.getValue.foreach { pair =>

        //Setting adjacent nodes
        bitSet = new util.BitSet(numberOfVertices)
        pair._2.foreach(bitSet.set(_))

        //Add the adjacent nodes with the key connecting them (key: pair._1)
        keyToAdj.put(pair._1, bitSet)
      }

      //Add a new edge
      edgeGraph.put(e.getKey, keyToAdj)
    }
  }

  /**
    * Adds a new vertex(scan) to the graph
    * @param vertex The new vertex
    * @param adj The list of the foreign keys and the connected nodes on each key
    * */

  protected def addVertex(vertex: Vertex, adj: List[(Int, Seq[Int])]): Unit ={
    numberOfVertices += 1
    vertices.put(vertex, adj)
    location.put(vertex.id, vertex.engines)
  }

  /**
    * The optimization of the graph is triggered by calling solve()
    */

  protected def solve(): DPJoinPlan = {
    for (vertex <- edgeGraph.descendingKeySet) {
      val b: util.BitSet = new util.BitSet(numberOfVertices)
      b.set(vertex.id)

      for (engine <- location.get(vertex.id)) {
        val scan = (scanClass.getConstructors()(0)).newInstance(vertex, engine, queryInfo).asInstanceOf[Scan]
        //val scan = new Scan(table = vertex, engine = engine)
        dptable.checkAndPut(engine , b, scan)
      }
    }

    for (vertex <- edgeGraph.descendingKeySet) {
      val b: util.BitSet = new util.BitSet(numberOfVertices)
      b.set(vertex.id)
      emitCsg(b)
      val bv: util.BitSet = new util.BitSet(numberOfVertices)
      for (i <- 1 to vertex.id) {
          bv.set(i)
      }

      enumerateCsgRec(b, bv)
    }
    val b: util.BitSet = new util.BitSet(numberOfVertices)

    for (i <- 1 to numberOfVertices) {
        b.set(i)
    }
    return dptable.getOptimalPlan(b)
  }

  protected def enumerateCsgRec(b: util.BitSet, bv: util.BitSet) {
    val N: util.BitSet = neighBoor(b, bv)
    if (N.isEmpty) return
    var powerSet: PowerSet = new PowerSet(N)
    while (powerSet.hasNext) {
      val t = powerSet.next
      if (!t.isEmpty) {
        t.or(b)
        //System.out.println("Check DPtable: "+t);
        if (dptable.getAllPlans(t) != null) emitCsg(t)
      }
    }
    powerSet = new PowerSet(N)

    while (powerSet.hasNext) {
      val t = powerSet.next
      if (!t.isEmpty) {
        t.or(b)
        val Xnew: util.BitSet = new util.BitSet(numberOfVertices)
        Xnew.or(bv)
        Xnew.or(N)
        enumerateCsgRec(t, Xnew)
      }
    }
  }

  protected def emitCsg(s1: util.BitSet) {
    val X: util.BitSet = new util.BitSet(numberOfVertices)
    val mins1: Int = s1.nextSetBit(0)

    for (i <- 1 to mins1) {
      X.set(i)
    }
    X.or(s1)
    val N: util.BitSet = neighBoor(s1, X)

    for (i <- N.size to 1 by -1) {
      val s2: util.BitSet = new util.BitSet(numberOfVertices)
      if (N.get(i)) {
        s2.set(i)

        //removed check for connectedness
        emitCsgCmp(s1, s2)
        enumerateCmpRec(s1, s2, X)
      }
    }
  }

  protected def enumerateCmpRec(s1: util.BitSet, s2: util.BitSet, X: util.BitSet) {
    var N: util.BitSet = neighBoor(s2, X)
    if (N.isEmpty) return
    var powerSet: PowerSet = new PowerSet(N)

    while (powerSet.hasNext) {
      val t = powerSet.next
      if (!t.isEmpty) {
        t.or(s2)
        //System.out.println("Check DPtable: "+t);
        if (dptable.getAllPlans(t) != null) emitCsgCmp(s1, t)
      }
    }
    X.or(N)
    N = neighBoor(s2, X)
    if (N.isEmpty) {
      return
    }
    powerSet = new PowerSet(N)

    while (powerSet.hasNext) {
      val t = powerSet.next
      if (!t.isEmpty) {
        t.or(s2)
        enumerateCmpRec(s1, t, X)
      }
    }
  }

  protected def emitCsgCmp(s1: util.BitSet, s2: util.BitSet) {
    //System.out.println("EmitCsgCmp s1:" + s1 + " s2: " + s2)
    val move = moveClass.getConstructors()(0)
    val join = joinClass.getConstructors()(0)
    val vars: HashSet[Int] = findJoinVars(s1, s2)
    val s: util.BitSet = new util.BitSet(numberOfVertices)
    s.or(s1)
    s.or(s2)
    totalChecks += 1

    for (leftSubPlan <- dptable.getAllPlans(s1).entrySet) {
      for (rightSubPlan <- dptable.getAllPlans(s2).entrySet) {
        if (leftSubPlan.getKey.equals(rightSubPlan.getKey)) {
          // leftSubPlan and rightSubPlan are on the same engine.

          // No move required
          val r: DPJoinPlan = join.newInstance(leftSubPlan.getValue, rightSubPlan.getValue, vars,
            leftSubPlan.getKey, queryInfo).asInstanceOf[Join]
          //val r: DPJoinPlan = new Join(left = leftSubPlan.getValue, right = rightSubPlan.getValue, vars = vars, engine = leftSubPlan.getKey)
          dptable.checkAndPut(leftSubPlan.getKey, s, r)
        } else {
          /* A move is required */
          /* Move left to right */
          var m: DPJoinPlan = move.newInstance(leftSubPlan.getValue, rightSubPlan.getKey, queryInfo).asInstanceOf[Move]
          var r: DPJoinPlan = join.newInstance(m, rightSubPlan.getValue, vars,
            rightSubPlan.getKey, queryInfo).asInstanceOf[Join]
          dptable.checkAndPut(rightSubPlan.getKey, s, r)
          /* Move right to left */
          m = move.newInstance(rightSubPlan.getValue, leftSubPlan.getKey, queryInfo).asInstanceOf[Move]
          r = join.newInstance(leftSubPlan.getValue, m, vars, leftSubPlan.getKey, queryInfo).asInstanceOf[Join]
          dptable.checkAndPut(leftSubPlan.getKey, s, r)

          /* Move all to other engine */
          for (engine <- dptable.engines) {
            var m1: DPJoinPlan = leftSubPlan.getValue
            var m2: DPJoinPlan = rightSubPlan.getValue
            if (!leftSubPlan.getKey.equals(engine)) {
              m1 = move.newInstance(m1, engine, queryInfo).asInstanceOf[Move]
            }

            if (!rightSubPlan.getKey.equals(engine)) {
              m2 = move.newInstance(m2, engine, queryInfo).asInstanceOf[Move]
            }

            r = join.newInstance(m1, m2, vars, engine, queryInfo).asInstanceOf[Join]
            dptable.checkAndPut(engine, s, r)
          }
        }
      }
      //check for grouping filters on subgraphs
    }
  }

  protected def findJoinVars(s1: util.BitSet, s2: util.BitSet): HashSet[Int] = {
    val ret = new HashSet[Int]
    for (i <- edgeGraph.keySet()) {
      if (s1.get(i.id)) {
        for (s <- edgeGraph.get(i).entrySet) {
          if (s.getValue.intersects(s2)) ret.add(s.getKey)
        }
      }
    }
    ret
  }

  def neighBoor(S: util.BitSet, X: util.BitSet): util.BitSet = {
    val N: util.BitSet = new util.BitSet(numberOfVertices)
    for (i <- edgeGraph.keySet()) {
      if (S.get(i.id)) {
        for (s <- edgeGraph.get(i).entrySet) {
          N.or(s.getValue)
        }
      }
    }

    N.andNot(X)
    N
  }
}
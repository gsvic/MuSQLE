package gr.cslab.ece.ntua.musqle.plan.hypergraph

import gr.cslab.ece.ntua.musqle.engine.Engine

/**
  * Created by vic on 12/10/2016.
  */
abstract class Vertex(val engines: Seq[Engine]) extends Comparable[Vertex] {
  final val id: Int = Vertex.id
  Vertex.id += 1

  override def compareTo(t: Vertex): Int = {
    this.id.compareTo(t.id)
  }
}

case class VertexImpl(override val engines: Seq[Engine]) extends Vertex(engines){
  private var name: String = s"Table$id"
  def setName(name: String): Vertex = {
    this.name = name
    this
  }

  override def toString: String = name
}

object Vertex{
  private var id: Int = 1
}
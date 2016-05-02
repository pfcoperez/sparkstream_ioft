package com.stratio.ioft.domain.measures

object VectorMeasure {

  implicit def vector3dToTriplet[T](vector: Vector[T]): (T, T, T) = vector match {
    case Vector(x, y, z) => (x, y, z)
  }

  implicit def triplet2vector3d[T](vectorMeasure: VectorMeasure[T]): Vector[T] =
    vectorMeasure.productIterator.map { case v: T @ unchecked => v } toVector

}

trait VectorMeasure[T] extends Product {
  def toVector: Vector[T] = productIterator.map { case d: T @ unchecked => d } toVector
}

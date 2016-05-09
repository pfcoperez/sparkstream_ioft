package com.stratio.ioft.domain.measures

object VectorMeasure {

  /**
    * Implicit conversion which transforms 3d vectors into triplets
    *
    * @param vector Should have just 3 elements.
    * @tparam T Scalar magnitude type.
    * @return A triplet with the 3 vector elements.
    */
  implicit def vector3dToTriplet[T](vector: Vector[T]): (T, T, T) = vector match {
    case Vector(x, y, z) => (x, y, z)
  }

  implicit def triplet2vector3d[T](vectorMeasure: VectorMeasure[T]): Vector[T] =
    vectorMeasure.productIterator.map { case v: T @ unchecked => v } toVector

}

/**
  *
  * [[VectorMeasure]] trait is aimed to be mixed with case classes which represent vector magnitudes
  * thus enabling mechanisms to easilly transform them into [[Vector]]s and vice versa.
  *
  * @tparam T Scalar magnitude of the vectorized product elements.
  */
trait VectorMeasure[T] extends Product {
  def toVector: Vector[T] = productIterator.map { case d: T @ unchecked => d } toVector
}

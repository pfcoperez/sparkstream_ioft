package com.stratio.ioft.util

object Math {

  import Implicits.Numeric

  type MagnitudeInTime = (BigInt, Double)

  def numericDerivation(f: Seq[MagnitudeInTime]): Seq[MagnitudeInTime] = {
    (f.view zip f.view.drop(1)) map {
      case ((t0, v0), (t1, v1)) => t1 -> (v1 - v0)/(t1 - t0).toLong
    }
  }

  object Implicits {

    trait Numeric[T] {
      val x: T
      val sumIdentity: T
      val mulIdentity: T
      def *(b: T): T
      def +(b: T): T
    }

    implicit def double2numeric(v: Double): Numeric[Double] = new Numeric[Double] {
      override val sumIdentity: Double = 0.0
      override val mulIdentity: Double = 1.0
      override val x: Double = v
      override def *(b: Double): Double = x*b
      override def +(b: Double): Double = x+b
    }

    implicit def int2numeric(v: Int): Numeric[Int] = new Numeric[Int] {
      override val sumIdentity: Int = 0
      override val mulIdentity: Int = 1
      override val x: Int = v
      override def *(b: Int): Int = x*b
      override def +(b: Int): Int = x+b
    }

    implicit def long2numeric(v: Long): Numeric[Long] = new Numeric[Long] {
      override val sumIdentity: Long = 0L
      override val mulIdentity: Long = 1L
      override val x: Long = v
      override def *(b: Long): Long = x*b
      override def +(b: Long): Long = x+b
    }

  }

  object LinAlg {

    def transpose[T](v: Vector[T]): Vector[Vector[T]] = v.transpose((x: T) => Vector(x))
    def transpose[T](m: Vector[Vector[T]]): Vector[Vector[T]] = m.transpose

    def multiply[T](
                     A: Vector[Vector[T]],
                     B: Vector[Vector[T]]
                   )(implicit toNumeric: T => Numeric[T]): Vector[Vector[T]] = {
      require(A.length == B.head.length)
      for(i <- 0 until A.length) yield {
        for(j <- 0 until A.head.length) yield
          (A.head.head.sumIdentity /: (0 until A.length)) { (s: T, t) =>
            s + A(i)(t)*B(t)(j)
          }
      } toVector
    } toVector

  }

  object Geometry {

  }

}

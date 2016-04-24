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

    def transpose[T](m: Vector[Vector[T]]): Vector[Vector[T]] = m.transpose

    def multiply[T <% Numeric[T]](A: Vector[Vector[T]], B: Vector[Vector[T]]): Vector[Vector[T]] = {
      require(A.length == B.head.length)
      for(i <- (0 until A.length).toVector) yield {
        for(j <- (0 until A.head.length).toVector) yield
          (A.head.head.sumIdentity /: (0 until A.length)) { (s: T, t) =>
            s + A(i)(t)*B(t)(j)
          }
      }
    }

    def identity[T <% Numeric[T]](n: Int, e: T): Vector[Vector[T]] =
      Vector.tabulate(n,n) {
        case (i,j) if i == j => e
        case _ => e.sumIdentity
      }

    def transform[T <% Numeric[T]](transformationMatrix: Vector[Vector[T]], v: Vector[T]): Vector[T] =
      transpose(multiply(transformationMatrix, transpose(Vector(v)))).head

  }

  object Geometry {

    import scala.math.{Pi, sin, cos}
    import Implicits.Numeric
    import LinAlg._

    def degrees2rads(degrees: Double): Double = Pi*degrees/180.0

    def rotateAboutX(rads: Double, v: Vector[Double]): Vector[Double] = {
      val rotMatrix = Vector(
        Vector(1.0,   0.0,              0.0),
        Vector(0.0,   cos(rads), -sin(rads)),
        Vector(0.0,   sin(rads),  cos(rads))
      )
      transform(rotMatrix, v)
    }

    def rotateAboutY(rads: Double, v: Vector[Double]): Vector[Double] = {
      val rotMatrix = Vector(
        Vector(cos(rads),  0.0,  sin(rads)),
        Vector(0.0,        1.0,        0.0),
        Vector(-sin(rads), 0.0,   cos(rads))
      )
      transform(rotMatrix, v)
    }

    def rotateAboutZ(rads: Double, v: Vector[Double]): Vector[Double] = {
      val rotMatrix = Vector(
        Vector(cos(rads), -sin(rads), 0.0),
        Vector(sin(rads), cos(rads),  0.0),
        Vector(0.0,             0.0,  1.0)
      )
      transform(rotMatrix, v)
    }

    def rotate(angles: (Double, Double, Double), v: Vector[Double]): Vector[Double] =
      rotateAboutZ(angles._3, rotateAboutY(angles._2, rotateAboutX(angles._1, v)))

  }

}

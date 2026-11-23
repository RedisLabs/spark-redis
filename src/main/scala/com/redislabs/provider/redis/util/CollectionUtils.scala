package com.redislabs.provider.redis.util

/**
  * @author The Viet Nguyen
  */
object CollectionUtils {

  /**
    * `distinctBy` for Scala 2.12. Scala 2.13 collections have their own `distinctBy`, which takes precedence over
    * this one.
    */
  implicit class RichCollection[A](val xs: Seq[A]) extends AnyVal {

    /**
      * @return the elements of the collection without duplicates according to `f`, keeping the first occurrence
      */
    def distinctBy[B](f: A => B): Seq[A] = {
      val builder = Seq.newBuilder[A]
      var seen = Set[B]()
      xs.foreach { element =>
        val distinctField = f(element)
        if (!seen(distinctField)) {
          seen += distinctField
          builder += element
        }
      }
      builder.result()
    }
  }

}

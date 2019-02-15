package com.redislabs.provider.redis.util

import scala.collection.IterableLike
import scala.collection.generic.CanBuildFrom

/**
  * @author The Viet Nguyen
  */
object CollectionUtils {

  implicit class RichCollection[A, Repr](val xs: IterableLike[A, Repr]) extends AnyVal {

    def distinctBy[B, That](f: A => B)(implicit cbf: CanBuildFrom[Repr, A, That]): That = {
      val builder = cbf(xs.repr)
      val iterator = xs.iterator
      var set = Set[B]()
      while (iterator.hasNext) {
        val element = iterator.next
        val distinctField = f(element)
        if (!set(distinctField)) {
          set += distinctField
          builder += element
        }
      }
      builder.result
    }
  }

}

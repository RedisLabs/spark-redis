package com.redislabs.provider.redis.util

import scala.collection.IterableOps
import scala.collection.BuildFrom

/**
  * @author The Viet Nguyen
  */
object CollectionUtils {

  implicit class RichCollection[A, Repr](val xs: IterableOps[A, Iterable, Repr]) extends AnyVal {

    def distinctBy[B, That](f: A => B)(implicit cbf: BuildFrom[Repr, A, That]): That = {
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

package com.redislabs.provider


import com.redislabs.provider.redis.NodesInfo._
import com.redislabs.provider.redis.RedisConnectionParameters

class RedisConfig(redisConnectionParameters: RedisConnectionParameters) extends Serializable {
  val nodes: java.util.ArrayList[RedisConnectionParameters] = new java.util.ArrayList[RedisConnectionParameters]

  getNodes(redisConnectionParameters).foreach(x => nodes.add(x._1))

  /**
   *
   * @param sPos start position of a slots range
   * @param ePos end position of a slots range
   * @return list of nodes(addr, port, index, range, startSlot, endSlot), where Inter([startSlot, endSlot], [sPos, ePos]) is not Nil(master only now)
   */
  def getNodesBySlots(sPos: Int, ePos: Int) = {
    def inter(sPos1: Int, ePos1: Int, sPos2: Int, ePos2: Int):Boolean = {
      if (sPos1 <= sPos2)
        return ePos1 >= sPos2
      else
        return ePos2 >= sPos1
    }
    val node = nodes.get(scala.util.Random.nextInt().abs % nodes.size())
    getSlots(node).filter(node => inter(sPos, ePos, node._4, node._5)).filter(_._2 == 0) //master only now
  }
}

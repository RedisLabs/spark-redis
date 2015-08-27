package com.redislabs.provider


import com.redislabs.provider.redis.NodesInfo._

class RedisConfig(ip: String, port: Int) extends Serializable {
  val nodes: java.util.ArrayList[(String, Int)] = new java.util.ArrayList[(String, Int)]

  getNodes((ip, port)).foreach(x => nodes.add((x._1, x._2)))

  def getNodesBySlots(sPos: Int, ePos: Int) = {
    def inter(sPos1: Int, ePos1: Int, sPos2: Int, ePos2: Int):Boolean = {
      if (sPos1 <= sPos2)
        return ePos1 >= sPos2
      else
        return ePos2 >= sPos1
    }
    val node = nodes.get(scala.util.Random.nextInt().abs % nodes.size())
    getSlots((node._1, node._2)).filter(node => inter(sPos, ePos, node._5, node._6))
  }
}
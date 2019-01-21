package com.github.trembita.akka

import akka.stream._
import akka.stream.stage._
import akka.stream.FlowShape

class SpanByFlow[K, V, Comb](init: V => Comb, addValue: (Comb, V) => Comb) extends GraphStage[FlowShape[(K, V), (K, Comb)]] {
  val in                                  = Inlet[(K, V)]("SpanByKeyFlow.in")
  val out                                 = Outlet[(K, Comb)]("SpanByKeyFlow.out")
  val shape: FlowShape[(K, V), (K, Comb)] = FlowShape(in, out)

  sealed trait CombBuilder
  case object Empty                                                       extends CombBuilder
  case class Building(k: K, c: Comb)                                      extends CombBuilder
  case class Done(receivedKey: K, doneComb: Comb, newKey: K, newValue: V) extends CombBuilder

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {
      private var combBuilder: CombBuilder = Empty

      setHandler(
        in,
        new InHandler {
          override def onPush(): Unit = {
            val (key, value) = grab(in)
            combBuilder match {
              case Empty =>
                combBuilder = Building(key, init(value))
                pull(in)

              case Building(`key`, comb) =>
                combBuilder = Building(key, addValue(comb, value))
                pull(in)

              case Building(otherKey, complitedComb) =>
                if (isAvailable(out)) {
                  pushKeyWithComb(otherKey, complitedComb, newKey = key, newValue = value)
                  pull(in)
                } else {
                  combBuilder = Done(otherKey, complitedComb, newKey = key, newValue = value)
                }
            }
          }

          override def onUpstreamFinish(): Unit = {
            if (isAvailable(out)) {
              combBuilder match {
                case Building(keyToPush, combToPush)   => push(out, keyToPush -> combToPush)
                case Done(keyToPush, combToPush, _, _) => push(out, keyToPush -> combToPush)
                case _                                 =>
              }
            }
            complete(out)
            super.onUpstreamFinish()
          }
        }
      )

      setHandler(
        out,
        new OutHandler {
          override def onPull(): Unit = {
            combBuilder match {
              case Done(receivedKey, doneComb, newKey, newValue) =>
                pushKeyWithComb(receivedKey, doneComb, newKey, newValue)
              case _ => //nothing to push
            }
            if (!hasBeenPulled(in)) pull(in)

          }
        }
      )

      private def pushKeyWithComb(keyToPush: K, combToPush: Comb, newKey: K, newValue: V): Unit = {
        push(out, keyToPush -> combToPush)
        combBuilder = Building(newKey, init(newValue))
      }
    }
}

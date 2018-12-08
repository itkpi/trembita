package com.github.trembita.examples.trips

import java.time.LocalDateTime
import com.github.trembita._
import com.github.trembita.ql._
import GroupingCriteria._
import AggDecl._
import AggRes._
import Common._
import Aliases._
import cats.implicits._
import cats.effect._
import com.github.trembita.cassandra.phantom.PhantomSource

class SampleReport(unitMessagesRepository: UnitMessagesRepository) {
  def generate(unitIds: List[String],
               fromDate: LocalDateTime,
               toDate: LocalDateTime) = {

    val messagesPipeline: DataPipelineT[IO, UnitMessage, Execution.Sequential] =
      PhantomSource.applyF(unitMessagesRepository.connector)(
        unitMessagesRepository.selectByUnits(unitIds, fromDate, toDate)
      )

    val result = getActivities(messagesPipeline)
      .to[Execution.Parallel]
      .query(
        _.filter(_.ignitionOn)
          .groupBy(
            r =>
              r.unitId.:@[`unit id`] &::
                r.driverId.:@[`driver id`] &::
                r.tripType.:@[`trip type`] &:: GNil
          )
          .aggregate(
            r =>
              r.coveredDistance.:@[`covered distance`].sum %::
                r.activityDuration.:@[`time in trip`].avg %::
                r.coveredDistance.:@[`max covered distance`].max %:: DNil
          )
          .orderAggregations
      )

    result
  }
}

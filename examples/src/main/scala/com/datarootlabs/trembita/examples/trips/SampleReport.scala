package com.datarootlabs.trembita.examples.trips

import java.time.LocalDateTime
import com.datarootlabs.trembita._
import ql._
import scala.util.Try
import Common._
import Aliases._
import GroupingCriteria._
import AggDecl._
import com.datarootlabs.trembita.cassandra.phantom.PhantomSource
import cats.implicits._


class SampleReport(unitMessagesRepository: UnitMessagesRepository) {
  def generate(unitIds: List[String], fromDate: LocalDateTime, toDate: LocalDateTime) = {

    val messagesPipeline: DataPipeline[UnitMessage, Try, Finiteness.Finite, Execution.Sequential] =
      PhantomSource.applyF(unitMessagesRepository.connector)(unitMessagesRepository.selectByUnits(
        unitIds, fromDate, toDate
      ))

    val result = getActivities(messagesPipeline)
      .to[Execution.Parallel]
      .query(_
        .filter(_.ignitionOn)
        .groupBy(r =>
          r.unitId.:@[`unit id`] &::
            r.driverId.:@[`driver id`] &::
            r.tripType.:@[`trip type`] &:: GNil
        )
        .aggregate(r =>
          r.coveredDistance.:@[`covered distance`].sum %::
            r.activityDuration.:@[`time in trip`].avg %::
            r.coveredDistance.:@[`max covered distance`].max %:: DNil
        )
        .orderAggregations
      )

    result
  }
}

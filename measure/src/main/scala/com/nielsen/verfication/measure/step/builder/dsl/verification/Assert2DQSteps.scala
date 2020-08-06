package com.nielsen.verfication.measure.step.builder.dsl.verification

import com.nielsen.verfication.measure.Loggable
import com.nielsen.verfication.measure.configuration.dqdefinition.RuleParam
import com.nielsen.verfication.measure.configuration.enums.{AccuracyType, DistinctnessType}
import com.nielsen.verfication.measure.context.DQContext
import com.nielsen.verfication.measure.step.DQStep
import com.nielsen.verfication.measure.step.builder.dsl.transform.DistinctnessExpr2DQSteps

trait Assert2DQSteps extends DQStep with Serializable {
  val name: String = ""
  protected val emtptDQSteps = Seq[DQStep]()
  def execute(context: DQContext): Boolean
}

object Assert2DQSteps {
  private val emtptExpr2DQSteps = new Assert2DQSteps() {
    override def execute(context: DQContext): Boolean = {
      true
    }

    override val name: String = ""
  }

  def apply(context: DQContext,
            ruleParam: RuleParam
           ): Assert2DQSteps = {
    ruleParam.getDqType match {
      case AccuracyType => AccuracyAssertDQSteps(context,ruleParam)
      case DistinctnessType => DistinctnessAssertDQSteps(context,ruleParam)
      case _ => emtptExpr2DQSteps
    }
  }
}
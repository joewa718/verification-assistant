package com.nielsen.verfication.measure.step.builder.dsl.assertrule

import com.nielsen.verfication.measure.Loggable
import com.nielsen.verfication.measure.configuration.dqdefinition.RuleParam
import com.nielsen.verfication.measure.configuration.enums.AccuracyType
import com.nielsen.verfication.measure.context.DQContext
import com.nielsen.verfication.measure.step.DQStep

trait Assert2DQSteps extends DQStep with Serializable {
  val name: String = ""
  protected val emtptDQSteps = Seq[DQStep]()
  def execute(context: DQContext,ruleParam:RuleParam): Boolean
}

object Assert2DQSteps {
  private val emtptExpr2DQSteps = new Assert2DQSteps() {
    override def execute(context: DQContext,ruleParam:RuleParam): Boolean = {
      true
    }

    override val name: String = ""

    /**
     * @return execution success
     */
    override def execute(context: DQContext): Boolean = true
  }

  def apply(context: DQContext,
            ruleParam: RuleParam
           ): Assert2DQSteps = {
    ruleParam.getDqType match {
      case AccuracyType => AccuracyAssertDQSteps(context,ruleParam)
      case _ => emtptExpr2DQSteps
    }
  }
}
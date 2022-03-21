package com.startdataengineering

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

import com.startdataengineering.model.ServerLog

class FraudDetection extends KeyedProcessFunction[String, String, String]{

  private var loginState: ValueState[java.lang.Boolean] = _ //This is boolean flag used to denote if the account(denoted by accountId) is already logged in.
  private var prevLoginCountry: ValueState[java.lang.String] = _ //This is used to keep track of the originating country of the most recent login for an account.
  private var timerState: ValueState[java.lang.Long] = _ //This is used to keep track of the timer. For eg if a user logged in 10 min ago we should set the previous 2 states to empty since we are outside our 5 min time limit.

  @throws[Exception] //open method is executed first.
  override def open(parameters: Configuration): Unit = {
    val loginDescriptor = new ValueStateDescriptor("login-flag", Types.BOOLEAN) 
    loginState = getRuntimeContext.getState(loginDescriptor) //This is boolean flag used to denote if the account(denoted by accountId) is already logged in.

    val prevCountryDescriptor = new ValueStateDescriptor("prev-country", Types.STRING)
    prevLoginCountry = getRuntimeContext.getState(prevCountryDescriptor) //This is used to keep track of the originating country of the most recent login for an account.

    val timerStateDescriptor = new ValueStateDescriptor("timer-state", Types.LONG)
    timerState = getRuntimeContext.getState(timerStateDescriptor) //This is used to keep track of the timer. For eg if a user logged in 10 min ago we should set the previous 2 states to empty since we are outside our 5 min time limit.
  }
  //These states will only be created once when the task is instantiated.

  @throws[Exception] //for each input the processElement method gets executed
  override def processElement(    //This method defines how we process each individual element in the input data stream
                               value: String,
                               ctx: KeyedProcessFunction[String, String, String]#Context,
                               out: Collector[String]): Unit = {
    val logEvent: ServerLog = ServerLog.fromString(value)

    val isLoggedIn = loginState.value
    val prevCountry = prevLoginCountry.value

    if ((isLoggedIn != null) && (prevCountry != null)){
      if ((isLoggedIn == true) && (logEvent.eventType == "login")) {
        // if account already logged in and tries another login from another country, send alert event
        if (prevCountry != logEvent.locationCountry) {
          val alert: String = f"Alert eventID: ${logEvent.eventId}%s, " +
            f"violatingAccountId: ${logEvent.accountId}%d, prevCountry: ${prevCountry}%s, " +
            f"currentCountry: ${logEvent.locationCountry}%s"
          out.collect(alert)
        }
      }
    }
    else if (logEvent.eventType == "login"){
      // set login and set prev login country
      loginState.update(true)
      prevLoginCountry.update(logEvent.locationCountry)
      // as soon as the account user logs in, we set a timer for 5 min for this account ID
      // 5 * 60 * 1000L -> 5 min
      val timer = logEvent.eventTimeStamp + (5 * 60 * 1000L)
      ctx.timerService.registerProcessingTimeTimer(timer)
      timerState.update(timer)
    }
    if (logEvent.eventType == "log-out") {
      // reset prev login and country
      loginState.clear()
      prevLoginCountry.clear()

      // remove timer, if it is set
      val timer = timerState.value()
      if (timer != null){
        ctx.timerService.deleteProcessingTimeTimer(timer)
      }
      timerState.clear()
    }
  }

  @throws[Exception] //The onTimer method is a call back method that gets executed when a timer runs out
  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[String, String, String]#OnTimerContext,
                       out: Collector[String]): Unit = {
    timerState.clear()
    loginState.clear()
    prevLoginCountry.clear()
  }
}
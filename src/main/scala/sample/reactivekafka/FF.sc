import scala.math.BigDecimal.RoundingMode

val rate = BigDecimal.valueOf(30.0)
val lastRate = BigDecimal.valueOf(29)

((rate - lastRate) * 100.0 / rate).setScale(2, RoundingMode.UP)
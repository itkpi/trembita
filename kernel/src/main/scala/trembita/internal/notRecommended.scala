package trembita.internal

import scala.annotation.meta.{beanGetter, beanSetter, getter, setter}

@getter @setter @beanGetter @beanSetter
class notRecommended(message: String = "", because: String = "") extends scala.annotation.StaticAnnotation

@getter @setter @beanGetter @beanSetter
class internalAPI(because: String = "") extends scala.annotation.StaticAnnotation

@getter @setter @beanGetter @beanSetter
class experimental(because: String = "") extends scala.annotation.StaticAnnotation

import com.google.inject.AbstractModule
import reactivekafka.DemoLifecycle

class Module extends AbstractModule {

  override def configure(): Unit = {
    bind(classOf[DemoLifecycle]).asEagerSingleton()
  }
}

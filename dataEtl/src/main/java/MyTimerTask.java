import java.text.SimpleDateFormat;
import java.util.TimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MyTimerTask extends TimerTask {

  private static final Logger LOG = LoggerFactory.getLogger(MyTimerTask.class);

  @Override
  public void run() {
    LOG.info("MyTimerTask start is "+new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(this.scheduledExecutionTime()));
    DataEtl.scheduler("inc");
  }
}

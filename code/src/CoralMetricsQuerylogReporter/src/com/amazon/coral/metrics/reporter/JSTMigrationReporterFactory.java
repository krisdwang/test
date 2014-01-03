package com.amazon.coral.metrics.reporter;

/**
 * Creates {@code Reporter}s using a given {@code ReporterFactory} and
 * wraps them with a {@code JSTMigrationReporter}.
 *
 * @see JSTMigrationReporter
 *
 * @author Dave Killian <killian@amazon.com>
 */
public class JSTMigrationReporterFactory implements ReporterFactory {

  private final ReporterFactory internalFactory;

  /**
   */
  public JSTMigrationReporterFactory(ReporterFactory internalFactory) {
    if(internalFactory == null)
      throw new IllegalArgumentException();

    this.internalFactory = internalFactory;
  }

  /**
   */
  public Reporter newReporter() {
    return new JSTMigrationReporter(internalFactory.newReporter());
  }
}

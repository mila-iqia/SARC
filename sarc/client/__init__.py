from sarc.config import ConfigurationError, ScraperConfig, config

if isinstance(config(), ScraperConfig):
    raise ConfigurationError("sarc.client cannot be used in scraping mode.")

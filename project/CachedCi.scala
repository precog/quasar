import org.romanowski.hoarder.actions.CachedCI
import org.romanowski.hoarder.actions.ci.TravisPRValidation

object CustomTravisValidation extends TravisPRValidation() {
  override def shouldUseCache(): Boolean =
    super.shouldUseCache() || sys.env.get("TRAVIS_BUILD_STAGE_NAME").map(_ != "Compile").getOrElse(false)
}

object CachedCi extends CachedCI.PluginBase(CustomTravisValidation)

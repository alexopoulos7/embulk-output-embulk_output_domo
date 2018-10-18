Embulk::JavaPlugin.register_output(
  "embulk_output_domo", "org.embulk.output.embulk_output_domo.EmbulkOutputDomoOutputPlugin",
  File.expand_path('../../../../classpath', __FILE__))

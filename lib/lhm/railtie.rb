module Lhm
  class Railtie < Rails::Railtie
    initializer "lhm.test_setup" do
      if Rails.env.test? || Rails.env.development?
        Lhm.execute_inline!
      end
    end
  end
end

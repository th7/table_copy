# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'table_copy/version'

Gem::Specification.new do |spec|
  spec.name          = "table_copy"
  spec.version       = TableCopy::VERSION
  spec.authors       = ["TLH"]
  spec.email         = ["tylerhartland7@gmail.com"]
  spec.summary       = %q{ Move full tables between databases. }
  spec.description   = 'Move full Postgres tables between databases.'
  spec.homepage      = ""
  spec.license       = "MIT"

  spec.files         = `git ls-files -z`.split("\x0")
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_development_dependency "bundler", "~> 1.6"
  spec.add_development_dependency "rake"
  spec.add_development_dependency "rspec", '~> 3.0'

  spec.add_development_dependency "pg", '~> 0.17.1'
end

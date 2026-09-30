terraform {

  backend "gcs" {

    bucket = "bucket280926-tfstate"

    prefix = "terraform/state"
  }
}
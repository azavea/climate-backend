provider "aws" {
  region = "us-east-1"
}

variable "key_name" {
  type        = "string"
  description = "The name of the EC2 secret key (primarily for SSH access)"
}

resource "aws_security_group" "security_group" {
  ingress {
    from_port = 0
    to_port   = 0
    protocol  = "-1"
    self      = true
  }

  ingress {
    from_port   = "22"
    to_port     = "22"
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_instance" "benchmark" {
  ami             = "ami-55ef662f"
  instance_type   = "c3.2xlarge"
  key_name        = "${var.key_name}"
  security_groups = ["${aws_security_group.security_group.name}"]

  tags {
    Name = "Benchmark"
  }
}

output "public_dns" {
   value = "${aws_instance.benchmark.public_dns}"
}

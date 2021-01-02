resource "aws_vpc" "main" {
  cidr_block = "172.32.128.0/22"
  tags = {
    Name = var.project_name
    Project = var.project_name
  }
}

resource "aws_internet_gateway" "main" {
  vpc_id = aws_vpc.main.id
  tags = {
    Project = var.project_name
  }
}

resource "aws_route_table" "main" {
  vpc_id = aws_vpc.main.id
  tags = {
    Project = var.project_name
  }

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.main.id
  }
}

resource "aws_subnet" "subnet" {
  vpc_id     = aws_vpc.main.id
  count = var.az_count
  availability_zone = data.aws_availability_zones.available.names[count.index]
  cidr_block  = cidrsubnet(aws_vpc.main.cidr_block, 2, count.index)
  #  cidr_block = "172.32.0.0/20"
  map_public_ip_on_launch = true
  tags = {
    Project = var.project_name
  }
}

resource "aws_route_table_association" "a" {
  count = var.az_count
  subnet_id = element(aws_subnet.subnet.*.id, count.index)
  route_table_id = element(aws_route_table.main.*.id, count.index)
}

resource "aws_network_acl" "main" {
  vpc_id = aws_vpc.main.id
  tags = {
    Project = var.project_name
  }
}

resource "aws_security_group" "outbound_only" {
  name = "${var.project_name}-outbound-only"
  vpc_id = aws_vpc.main.id
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  tags = {
    Project = var.project_name
  }
}
# Akton Resource Name (QRN) System

## Overview

The Akton Resource Name (QRN) is a structured identifier used within the Akton framework to uniquely identify and manage hierarchical actors across different services and partitions. QRNs are designed to reflect the hierarchical relationships of actors within the system, facilitating effective management, security, and operational oversight.

## QRN Structure

A QRN is composed of several parts, each representing a specific aspect of the resource:

`arn:partition:service:account-id:hierarchy/path`


### Components

- **arn**: Indicates that the string is a Akton Resource Name.
- **partition**: Classifies the resource as internal or external (`akton-internal`, `akton-external`).
- **service**: Specifies the service within Akton that the actor belongs to.
- **account-id**: Identifies the owner or account responsible for the actor.
- **hierarchy/path**: Provides a path-like structure that shows the actor's position within the tree, reflecting parent-child relationships.

## Examples

### Corporate Hierarchy Actor

`arn:akton-internal:hr:company123:root/departmentA/team1`


This QRN identifies an actor representing Team 1, which is part of Department A under the HR service, managed by account `company123`.

### IoT Device in a Network Topology

`arn:akton-external:iot:vendor456:root/region1/building5/floor3/device42`


This QRN points to Device 42 located on Floor 3 of Building 5 in Region 1, managed by IoT services for the vendor account `vendor456`.

## Usage

### Path Construction

When adding new actors to the system, construct their QRN by appending to the parent's QRN path, ensuring each actor’s QRN accurately reflects their position within the hierarchy.

### Dynamic Tree Manipulation

If an actor is moved within the hierarchy, update their QRN—and potentially those of all descendants—to reflect the new path. This keeps the identification consistent and meaningful.

### Resource Management

Use QRNs for logging, access control, and management tools to monitor interactions, manage permissions, and track activities based on actors' hierarchical locations.

## Conclusion

The QRN system provides a robust method for uniquely identifying and managing actors within a complex, hierarchical structure, supporting enhanced security, operational management, and clarity throughout the Akton framework.

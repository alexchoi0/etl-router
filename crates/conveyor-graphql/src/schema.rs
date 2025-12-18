use async_graphql::{Schema, EmptySubscription};

use super::resolvers::{QueryRoot, MutationRoot};

pub type RouterSchema = Schema<QueryRoot, MutationRoot, EmptySubscription>;

pub fn create_schema() -> RouterSchema {
    Schema::build(QueryRoot, MutationRoot, EmptySubscription)
        .finish()
}

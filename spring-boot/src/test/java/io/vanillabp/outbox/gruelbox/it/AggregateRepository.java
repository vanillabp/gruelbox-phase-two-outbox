package io.vanillabp.outbox.gruelbox.it;

import org.springframework.data.jpa.repository.JpaRepository;

public interface AggregateRepository extends JpaRepository<Aggregate, Long> {

}

"""The policy corpus: 40 clauses for Juniper Air, an airline that doesn't exist.

Eight policy types, five clauses each. Two clauses are easy to mix up on purpose,
the way the pair in Moffatt v. Air Canada was:

  REF-03  refund form: "within 90 days of the date your ticket was issued"
  BRV-02  bereavement: "cannot be applied to a ticket after travel has been completed"
"""
from typing import NamedTuple


class Seed(NamedTuple):
    clause_id: str
    policy_type: str
    owner: str        # the team that signs off the wording
    body: str


CLAUSES = [
    # --- bereavement ------------------------------------------------------------
    Seed("BRV-01", "bereavement", "Customer Relations",
         "Juniper Air offers reduced bereavement fares when you need to travel because of "
         "the death or imminent death of an immediate family member."),
    Seed("BRV-02", "bereavement", "Customer Relations",
         "A bereavement fare must be requested before travel by calling Reservations. "
         "The bereavement policy cannot be applied to a ticket after travel has been completed."),
    Seed("BRV-03", "bereavement", "Customer Relations",
         "For bereavement fares, immediate family means a spouse, child, parent, sibling, "
         "grandparent or grandchild, including in-laws and step-relatives."),
    Seed("BRV-04", "bereavement", "Customer Relations",
         "To confirm a bereavement fare, give the name of the deceased, your relationship to "
         "them and the phone number of the funeral home or hospital within 7 days of booking."),
    Seed("BRV-05", "bereavement", "Customer Relations",
         "Bereavement fares are offered only on flights operated by Juniper Air and must be "
         "booked no more than 10 days before departure."),
    # --- refund -----------------------------------------------------------------
    Seed("REF-01", "refund", "Customer Relations",
         "Flex and Business fares are refundable. A refundable fare canceled before departure "
         "is refunded in full to the original form of payment."),
    Seed("REF-02", "refund", "Customer Relations",
         "Basic fares are non-refundable. If you cancel a Basic fare, only the unused "
         "government taxes are returned."),
    Seed("REF-03", "refund", "Customer Relations",
         "To request a refund for an unused ticket, submit the Ticket Refund Application form "
         "within 90 days of the date your ticket was issued."),
    Seed("REF-04", "refund", "Customer Relations",
         "A ticket booked directly with Juniper Air can be canceled within 24 hours of purchase "
         "for a full refund, if the flight departs at least 7 days later."),
    Seed("REF-05", "refund", "Customer Relations",
         "An approved refund is paid to the original form of payment within 30 business days. "
         "Refunds are never paid in cash at the airport."),
    # --- baggage ----------------------------------------------------------------
    Seed("BAG-01", "baggage", "Baggage Services",
         "Each passenger may bring one carry-on bag of up to 10 kg and one personal item "
         "free of charge."),
    Seed("BAG-02", "baggage", "Baggage Services",
         "On Basic and Standard fares the first checked bag costs $35 and the second costs $50. "
         "Each checked bag may weigh up to 23 kg."),
    Seed("BAG-03", "baggage", "Baggage Services",
         "A checked bag that weighs more than 23 kg is charged an overweight fee of $100. "
         "Bags over 32 kg are not accepted as checked baggage."),
    Seed("BAG-04", "baggage", "Baggage Services",
         "Report a delayed or damaged bag at the airport baggage desk or online within 7 days "
         "of arrival. Claims filed later cannot be accepted."),
    Seed("BAG-05", "baggage", "Baggage Services",
         "Spare lithium batteries and power banks must travel in carry-on baggage. "
         "They are not allowed in checked bags."),
    # --- changes ----------------------------------------------------------------
    Seed("CHG-01", "changes", "Reservations",
         "A Standard fare can be changed for a $75 fee plus any fare difference, up to 2 hours "
         "before departure."),
    Seed("CHG-02", "changes", "Reservations",
         "A Basic fare cannot be changed once the 24-hour cancellation window has passed."),
    Seed("CHG-03", "changes", "Reservations",
         "Flex and Business fares can be changed without a change fee. A fare difference may "
         "still apply."),
    Seed("CHG-04", "changes", "Reservations",
         "A misspelled name can be corrected free of charge, up to 3 characters. A ticket "
         "cannot be transferred to another person."),
    Seed("CHG-05", "changes", "Reservations",
         "You can stand by for an earlier flight on the same day for a $25 fee when seats are "
         "available. Same-day standby is not offered on Basic fares."),
    # --- disruption -------------------------------------------------------------
    Seed("DLY-01", "disruption", "Operations Control",
         "If Juniper Air cancels a flight, passengers may choose rebooking on the next available "
         "flight at no extra cost or a refund of the unused ticket."),
    Seed("DLY-02", "disruption", "Operations Control",
         "For a delay of 3 hours or more that is within the airline's control, Juniper Air "
         "provides a meal voucher worth $15."),
    Seed("DLY-03", "disruption", "Operations Control",
         "For an overnight delay that is within the airline's control, Juniper Air provides "
         "hotel accommodation and ground transport to passengers who are away from home."),
    Seed("DLY-04", "disruption", "Operations Control",
         "A passenger who misses a connection because of a delay, with both flights on the same "
         "ticket, is rebooked on the next available flight free of charge."),
    Seed("DLY-05", "disruption", "Operations Control",
         "A passenger denied boarding on an oversold flight, who did not volunteer, receives "
         "$400 in compensation and rebooking on the next available flight."),
    # --- check-in ---------------------------------------------------------------
    Seed("CHK-01", "checkin", "Airport Services",
         "Online check-in opens 24 hours before departure and closes 60 minutes before "
         "departure."),
    Seed("CHK-02", "checkin", "Airport Services",
         "Checked bags must be dropped at the counter at least 45 minutes before departure on "
         "domestic flights and at least 60 minutes before departure on international flights."),
    Seed("CHK-03", "checkin", "Airport Services",
         "Boarding closes 15 minutes before departure. A passenger who reaches the gate later "
         "cannot board, and the ticket is treated as a no-show."),
    Seed("CHK-04", "checkin", "Airport Services",
         "International flights require a passport valid for at least 6 months after the "
         "travel date. Juniper Air cannot board a passenger without valid travel documents."),
    Seed("CHK-05", "checkin", "Airport Services",
         "Seat selection is free at check-in. Choosing a seat earlier costs $12 on Basic fares "
         "and is included in Standard, Flex and Business fares."),
    # --- special services -------------------------------------------------------
    Seed("SPC-01", "special", "Special Services",
         "A small cat or dog may travel in the cabin for a $95 fee, in a carrier that fits "
         "under the seat. Pet and carrier together may weigh up to 8 kg."),
    Seed("SPC-02", "special", "Special Services",
         "A child aged 5 to 11 who travels alone must use the unaccompanied minor service, "
         "which costs $100 each way. Children under 5 cannot travel alone."),
    Seed("SPC-03", "special", "Special Services",
         "Wheelchair and mobility assistance must be requested at least 48 hours before "
         "departure. The assistance is free of charge."),
    Seed("SPC-04", "special", "Special Services",
         "A trained service dog travels in the cabin free of charge. Emotional support animals "
         "are not accepted as service animals and travel as pets."),
    Seed("SPC-05", "special", "Special Services",
         "A passenger who travels with a portable oxygen concentrator must send a medical "
         "clearance form at least 72 hours before departure."),
    # --- loyalty ----------------------------------------------------------------
    Seed("LOY-01", "loyalty", "Loyalty Team",
         "Juniper Points expire after 18 months without any earning or redeeming activity."),
    Seed("LOY-02", "loyalty", "Loyalty Team",
         "Points for a past flight must be claimed within 6 months of the travel date. "
         "Older flights cannot be credited."),
    Seed("LOY-03", "loyalty", "Loyalty Team",
         "Silver status requires 25,000 status points in a calendar year and includes one free "
         "checked bag."),
    Seed("LOY-04", "loyalty", "Loyalty Team",
         "Points can be transferred to another member for a fee of $10 per 1,000 points. "
         "Transferred points cannot be refunded."),
    Seed("LOY-05", "loyalty", "Loyalty Team",
         "An award ticket can be canceled up to 24 hours before departure for a $50 redeposit "
         "fee. After that, the points are not returned."),
]

POLICY_TYPES = sorted({c.policy_type for c in CLAUSES})
